defmodule SymphonyElixir.ReviewWorkspace do
  @moduledoc "Prepares a dedicated reviewer clone using one contained check operation."

  alias SymphonyElixir.{WorkerOperation, WorkstreamCommand}

  @script ~S"""
  import ctypes, json, os, pathlib, re, subprocess, sys, tempfile
  try:
      p = json.loads(os.environ['WORKSTREAM_INPUTS_JSON'])
      source = pathlib.Path.cwd().resolve()
      root = pathlib.Path(p['root'])
      target = pathlib.Path(p['workspace'])
      assert root.is_absolute() and root.resolve() == root and root.is_dir()
      assert target.parent == root / '.factory-reviews' and target != source
      parent = target.parent
      parent.mkdir(mode=0o700, exist_ok=True)
      assert parent.resolve() == parent and not parent.is_symlink()
      gitdir = source / '.git'
      assert gitdir.is_dir() and not gitdir.is_symlink()
      assert not any((gitdir / n).exists() for n in ('commondir','config.worktree','objects/info/alternates'))
      assert not any(f.is_symlink() for f in gitdir.rglob('*'))
      env = {'PATH':'/usr/bin:/bin','HOME':'/nonexistent','LC_ALL':'C','GIT_CONFIG_NOSYSTEM':'1','GIT_CONFIG_GLOBAL':'/dev/null','GIT_CONFIG_SYSTEM':'/dev/null','GIT_TERMINAL_PROMPT':'0','GIT_ATTR_NOSYSTEM':'1'}
      argv = ['/usr/bin/git','--no-replace-objects','-c','core.hooksPath=/dev/null','-c','core.fsmonitor=false','-c','protocol.allow=never','-c','protocol.file.allow=always','-c','submodule.recurse=false']
      def git(cwd, *args):
          r = subprocess.run(argv + list(args), cwd=cwd, env=env, capture_output=True, text=True, timeout=60)
          assert r.returncode == 0, 'review Git operation failed'
          return r.stdout.strip()
      def publish(staged):
          libc = ctypes.CDLL(None, use_errno=True)
          if sys.platform.startswith('linux') and hasattr(libc, 'renameat2'):
              rename = libc.renameat2
              rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
              result = rename(-100, os.fsencode(staged), -100, os.fsencode(target), 1)
          elif sys.platform == 'darwin' and hasattr(libc, 'renamex_np'):
              rename = libc.renamex_np
              rename.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint]
              result = rename(os.fsencode(staged), os.fsencode(target), 4)
          else:
              raise OSError('exclusive atomic rename unavailable')
          if result != 0:
              raise OSError(ctypes.get_errno(), 'exclusive checkout publication failed')
      safe = re.compile(r'(?:core\.(?:repositoryformatversion|filemode|bare|logallrefupdates|ignorecase|precomposeunicode|hookspath|fsmonitor)|commit\.gpgsign|user\.(?:name|email)|extensions\.objectformat|remote\..+\.(?:url|fetch|pushurl)|branch\..+\.(?:remote|merge))')
      assert all(safe.fullmatch(n) for n in git(source,'config','--file',str(gitdir/'config'),'--no-includes','--name-only','--list').splitlines())
      assert git(source,'rev-parse','HEAD') == p['head_sha']
      assert git(source,'rev-parse',p['base_sha']+'^{commit}') == p['base_sha']
      git(source,'merge-base','--is-ancestor',p['base_sha'],p['head_sha'])
      marker = target / '.git' / 'factory-review.json'
      if target.exists() or target.is_symlink():
          assert target.is_dir() and target.resolve() == target and not target.is_symlink()
          assert marker.is_file() and not marker.is_symlink() and json.loads(marker.read_text()) == p
      else:
          staging = pathlib.Path(tempfile.mkdtemp(prefix='.'+target.name+'.prepare-',dir=parent))
          staged = staging / 'checkout'
          git(source,'clone','--no-hardlinks','--no-checkout','--template=','--',str(source),str(staged))
          git(staged,'checkout','--detach',p['head_sha'])
          assert git(staged,'rev-parse','HEAD') == p['head_sha']
          assert not git(staged,'status','--porcelain=v1')
          with (staged / '.git' / 'factory-review.json').open('x') as record:
              record.write(json.dumps(p,sort_keys=True))
              record.flush()
              os.fsync(record.fileno())
          publish(staged)
      final_gitdir = target / '.git'
      assert final_gitdir.is_dir() and not final_gitdir.is_symlink()
      assert not any((final_gitdir / n).exists() for n in ('commondir','config.worktree','objects/info/alternates'))
      assert not any(f.is_symlink() for f in final_gitdir.rglob('*'))
      assert git(target,'rev-parse','HEAD') == p['head_sha']
      assert not git(target,'status','--porcelain=v1')
      print(json.dumps(p,sort_keys=True))
  except (AssertionError, KeyError, ValueError, OSError, subprocess.SubprocessError):
      print('REVIEW_CHECKOUT_FAILED',file=sys.stderr)
      sys.exit(1)
  """

  @spec prepare(map(), map(), keyword()) :: {:ok, map()} | {:error, term()} | {:uncertain, term()}
  def prepare(run, identity, opts) do
    operation = run.operations[run.current_attempt_id]
    stable_id = if operation, do: operation.side_effect_id, else: run.current_attempt_id
    root = Keyword.fetch!(opts, :workspace_root)
    name = :crypto.hash(:sha256, run.id <> "\n" <> stable_id) |> Base.encode16(case: :lower)
    workspace = Path.join([root, ".factory-reviews", name])
    pins = Map.merge(identity, %{root: root, workspace: workspace, operation_id: stable_id})
    command = ["/usr/bin/python3", "-I", "-c", @script]

    result =
      case opts[:worker_control] do
        nil -> WorkstreamCommand.run(command, run.workspace, 120_000, pins)
        control -> WorkerOperation.run_check(command, run.workspace, 120_000, run.current_attempt_id, control, opts[:on_process_start], pins)
      end

    case result do
      {:ok, %{exit_status: 0, truncated: false, timed_out: false, output: output}} ->
        with {:ok, actual} <- Jason.decode(String.trim(output)), true <- actual == Jason.decode!(Jason.encode!(pins)) do
          {:ok, pins}
        else
          _ -> {:error, :review_checkout_not_verified}
        end

      {:uncertain, _} = uncertain ->
        uncertain

      _ ->
        {:error, :review_checkout_failed}
    end
  end
end
