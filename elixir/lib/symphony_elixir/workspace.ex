defmodule SymphonyElixir.Workspace do
  @moduledoc """
  Creates isolated per-issue workspaces for parallel Codex agents.
  """

  require Logger
  alias SymphonyElixir.{Config, PathSafety, SSH}

  @remote_workspace_marker "__SYMPHONY_WORKSPACE__"
  @remove_marker "__SYMPHONY_REMOVE__"
  @remove_script ~S"""
  import contextlib
  import json
  import os
  import shutil
  import signal
  import stat
  import subprocess
  import sys

  MARKER = "__SYMPHONY_REMOVE__"

  class Rejected(Exception):
      def __init__(self, reason, workspace=None, root=None):
          self.reason, self.workspace, self.root = reason, workspace, root


  def identity(info):
      return info.st_dev, info.st_ino, stat.S_IFMT(info.st_mode)


  def absence(path):
      # ENOENT is a no-op only if no dangling/looping ancestor link hides the path.
      try:
          os.lstat(path)
          return False
      except FileNotFoundError:
          ancestor = os.path.dirname(path)
          while ancestor != os.path.dirname(ancestor):
              if os.path.islink(ancestor) and not os.path.exists(ancestor):
                  raise Rejected("symlink_escape", path, os.path.dirname(ancestor))
              ancestor = os.path.dirname(ancestor)
          return True


  def mount_identity(fd):
      if sys.platform.startswith("linux"):
          with open("/proc/self/fdinfo/" + str(fd), encoding="ascii") as info:
              for line in info.read(8192).splitlines():
                  if line.startswith("mnt_id:"):
                      return line.split(":", 1)[1].strip()
          raise Rejected("mount_identity_unavailable")
      if sys.platform == "darwin":
          return os.fstat(fd).st_dev
      raise Rejected("unsupported_python")


  def check_mount(fd, expected):
      if mount_identity(fd) != expected:
          raise Rejected("mount_boundary")


  def verify_tree(fd, expected):
      with os.scandir(fd) as entries:
          for entry in entries:
              if entry.is_dir(follow_symlinks=False):
                  child = os.open(entry.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                  try:
                      check_mount(child, expected)
                      verify_tree(child, expected)
                  finally:
                      os.close(child)


  def parent_fd(stack, root_fd, parts, expected_mount=None):
      fd = root_fd
      chain = []
      for part in parts:
          fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
          stack.callback(os.close, fd)
          if expected_mount is not None:
              check_mount(fd, expected_mount)
          chain.append(identity(os.fstat(fd)))
      return fd, chain


  def supported_runtime():
      return (sys.version_info >= (3, 11) and
              getattr(shutil.rmtree, "avoids_symlink_attacks", False) and
              hasattr(os, "O_NOFOLLOW") and hasattr(os, "O_DIRECTORY") and
              hasattr(signal, "SIGALRM") and hasattr(signal, "setitimer") and
              (sys.platform.startswith("linux") or sys.platform == "darwin"))


  def remove(workspace, root, hook, timeout_ms, recorded):
      if not supported_runtime():
          raise Rejected("unsupported_python")
      requested = os.path.abspath(workspace)
      configured = os.path.abspath(root)
      physical_root = os.path.realpath(configured)
      physical_workspace = os.path.realpath(requested)
      if physical_root == "/":
          raise Rejected("unsafe_root", requested, physical_root)
      if physical_workspace == physical_root:
          raise Rejected("equals_root", physical_workspace, physical_root)
      missing = absence(requested)
      if missing and not recorded:
          return None
      if recorded:
          parent = os.path.realpath(os.path.dirname(requested))
          if not physical_workspace.startswith(parent + os.sep):
              raise Rejected("symlink_escape", requested, parent)
      if not physical_workspace.startswith(physical_root + os.sep):
          raise Rejected("outside_root", physical_workspace, physical_root)
      if missing:
          return None
      # A configured root may be an alias. No link below it is followed.
      for prefix in (configured, physical_root):
          if requested.startswith(prefix + os.sep):
              relative = requested[len(prefix) + 1:]
              break
      else:
          raise Rejected("symlink_escape", requested, physical_root)
      parts = relative.split(os.sep)
      if any(part in ("", ".", "..") for part in parts):
          raise Rejected("unsafe_component", requested, physical_root)
      with contextlib.ExitStack() as stack:
          filesystem_fd = os.open(os.sep, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
          stack.callback(os.close, filesystem_fd)
          root_fd, _ = parent_fd(stack, filesystem_fd, physical_root.strip(os.sep).split(os.sep))
          root_identity = identity(os.fstat(root_fd))
          allowed_mount = mount_identity(root_fd)
          parent, chain = parent_fd(stack, root_fd, parts[:-1], allowed_mount)
          name = parts[-1]
          initial = os.stat(name, dir_fd=parent, follow_symlinks=False)
          if stat.S_ISLNK(initial.st_mode):
              raise Rejected("symlink_escape", requested, physical_root)
          hook_result = None
          if stat.S_ISDIR(initial.st_mode):
              target_fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
              stack.callback(os.close, target_fd)
              check_mount(target_fd, allowed_mount)
              if identity(os.fstat(target_fd)) != identity(initial):
                  raise Rejected("path_changed", requested, physical_root)
              verify_tree(target_fd, allowed_mount)
          if hook and stat.S_ISDIR(initial.st_mode):
              os.fchdir(target_fd)
              process = subprocess.Popen(["sh", "-lc", hook], start_new_session=True,
                                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
              finished = False
              try:
                  status = process.wait(timeout=timeout_ms / 1000)
                  finished = True
                  if status:
                      hook_result = {"status": "failed", "exit_status": status}
              except subprocess.TimeoutExpired:
                  hook_result = {"status": "timeout"}
              finally:
                  if not finished:
                      # Reap the hook and kill ordinary descendants on timeout/deadline.
                      try:
                          os.killpg(process.pid, signal.SIGKILL)
                      except ProcessLookupError:
                          pass
                      process.wait(timeout=1)
          # Re-open every ancestor without following links and compare identities.
          if (os.path.realpath(configured) != physical_root or
                  identity(os.stat(physical_root, follow_symlinks=False)) != root_identity):
              raise Rejected("path_changed", requested, physical_root)
          current_parent, current_chain = parent_fd(stack, root_fd, parts[:-1], allowed_mount)
          if current_chain != chain or identity(os.stat(name, dir_fd=current_parent, follow_symlinks=False)) != identity(initial):
              raise Rejected("path_changed", requested, physical_root)
          # rmtree's fd implementation checks child identities and never follows links.
          if stat.S_ISDIR(initial.st_mode):
              # Scope every recursive open to this mount, including same-device
              # Linux bind mounts. Existing retained fds survive namespace swaps.
              original_open = os.open
              def guarded_open(*args, **kwargs):
                  opened = original_open(*args, **kwargs)
                  try:
                      if stat.S_ISDIR(os.fstat(opened).st_mode):
                          check_mount(opened, allowed_mount)
                      return opened
                  except BaseException:
                      os.close(opened)
                      raise
              os.open = guarded_open
              try:
                  shutil.rmtree(name, dir_fd=current_parent)
              finally:
                  os.open = original_open
          else:
              os.unlink(name, dir_fd=current_parent)
          return hook_result


  nonce = sys.argv[-1]
  result = {"status": "error"}
  result["nonce"] = nonce
  try:
      workspace, root, hook, timeout, recorded, nonce = sys.argv[1:]
      timeout_ms = int(timeout)
      if not supported_runtime():
          raise Rejected("unsupported_python")
      def deadline(_signal, _frame):
          raise Rejected("timeout")
      signal.signal(signal.SIGALRM, deadline)
      signal.setitimer(signal.ITIMER_REAL, (timeout_ms + 5000) / 1000)
      hook_result = remove(workspace, root, hook, timeout_ms, recorded == "true")
      result = {"status": "ok", "hook": hook_result, "nonce": nonce}
  except Rejected as error:
      result.update(reason=error.reason, workspace=error.workspace, root=error.root)
  except OSError as error:
      # No hook output, command, environment or file contents enter this protocol.
      result.update(reason="filesystem_error", errno=error.errno)
  except Exception:
      result.update(reason="helper_error")
  finally:
      if hasattr(signal, "setitimer"):
          signal.setitimer(signal.ITIMER_REAL, 0)
      print(MARKER + json.dumps(result, separators=(",", ":")))
  sys.exit(0 if result["status"] == "ok" else 1)
  """

  @type worker_host :: String.t() | nil

  @spec create_for_issue(map() | String.t() | nil, worker_host()) ::
          {:ok, Path.t()} | {:error, term()}
  def create_for_issue(issue_or_identifier, worker_host \\ nil) do
    issue_context = issue_context(issue_or_identifier)

    try do
      safe_id = workspace_key(issue_or_identifier)

      with {:ok, workspace} <- workspace_path_for_issue(safe_id, worker_host),
           :ok <- validate_workspace_path(workspace, worker_host),
           {:ok, workspace, created?} <- ensure_workspace(workspace, worker_host) do
        case maybe_run_after_create_hook(workspace, issue_context, created?, worker_host) do
          :ok ->
            {:ok, workspace}

          {:error, _reason} = error ->
            cleanup_failed_new_workspace(workspace, created?, worker_host)
            error
        end
      end
    rescue
      error in [ArgumentError, ErlangError, File.Error] ->
        Logger.error("Workspace creation failed #{issue_log_context(issue_context)} worker_host=#{worker_host_for_log(worker_host)} error=#{Exception.message(error)}")
        {:error, error}
    end
  end

  defp ensure_workspace(workspace, nil) do
    cond do
      File.dir?(workspace) ->
        {:ok, workspace, false}

      File.exists?(workspace) ->
        File.rm_rf!(workspace)
        create_workspace(workspace)

      true ->
        create_workspace(workspace)
    end
  end

  defp ensure_workspace(workspace, worker_host) when is_binary(worker_host) do
    script =
      [
        "set -eu",
        remote_shell_assign("workspace", workspace),
        "if [ -d \"$workspace\" ]; then",
        "  created=0",
        "elif [ -e \"$workspace\" ]; then",
        "  rm -rf \"$workspace\"",
        "  mkdir -p \"$workspace\"",
        "  created=1",
        "else",
        "  mkdir -p \"$workspace\"",
        "  created=1",
        "fi",
        "cd \"$workspace\"",
        "printf '%s\\t%s\\t%s\\n' '#{@remote_workspace_marker}' \"$created\" \"$(pwd -P)\""
      ]
      |> Enum.reject(&(&1 == ""))
      |> Enum.join("\n")

    case run_remote_command(worker_host, script, Config.settings!().hooks.timeout_ms) do
      {:ok, {output, 0}} ->
        parse_remote_workspace_output(output)

      {:ok, {output, status}} ->
        {:error, {:workspace_prepare_failed, worker_host, status, output}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp create_workspace(workspace) do
    File.rm_rf!(workspace)
    File.mkdir_p!(workspace)
    {:ok, workspace, true}
  end

  @spec remove(Path.t()) :: {:ok, [String.t()]} | {:error, term(), String.t()}
  def remove(workspace), do: remove(workspace, nil)

  @spec remove(Path.t(), worker_host()) :: {:ok, [String.t()]} | {:error, term(), String.t()}
  def remove(workspace, worker_host), do: guarded_remove(workspace, worker_host, false)

  @doc false
  @spec remove_recorded(Path.t(), worker_host()) :: {:ok, [String.t()]} | {:error, term(), String.t()}
  def remove_recorded(workspace, nil) when is_binary(workspace) do
    if Path.type(workspace) == :absolute,
      do: guarded_remove(workspace, nil, true),
      else: {:error, {:workspace_path_unreadable, workspace, :not_absolute}, ""}
  end

  def remove_recorded(workspace, worker_host) when is_binary(workspace) and is_binary(worker_host),
    do: guarded_remove(workspace, worker_host, true)

  def remove_recorded(workspace, _worker_host), do: {:error, {:workspace_path_unreadable, workspace, :invalid}, ""}

  @spec remove_issue_workspaces(term()) :: :ok | {:error, term(), String.t()}
  def remove_issue_workspaces(identifier), do: remove_issue_workspaces(identifier, nil)

  @spec remove_issue_workspaces(term(), worker_host()) :: :ok | {:error, term(), String.t()}
  def remove_issue_workspaces(%{id: _issue_id, identifier: _identifier} = issue, worker_host),
    do: remove_issue_paths(workspace_key(issue), worker_host)

  def remove_issue_workspaces(identifier, worker_host) when is_binary(identifier),
    do: remove_issue_paths(workspace_key(identifier), worker_host)

  def remove_issue_workspaces(_identifier, _worker_host), do: :ok

  defp remove_issue_paths(key, nil) do
    case Config.settings!().worker.ssh_hosts do
      [] ->
        remove_issue_path(key, nil)

      hosts ->
        Enum.reduce_while(hosts, :ok, fn host, :ok ->
          case remove_issue_path(key, host) do
            :ok -> {:cont, :ok}
            error -> {:halt, error}
          end
        end)
    end
  end

  defp remove_issue_paths(key, host) when is_binary(host), do: remove_issue_path(key, host)
  defp remove_issue_paths(_key, _host), do: :ok

  defp remove_issue_path(key, host) do
    with {:ok, workspace} <- workspace_path_for_issue(key, host) do
      case remove(workspace, host) do
        {:ok, _} -> :ok
        error -> error
      end
    else
      {:error, reason} -> {:error, reason, ""}
    end
  end

  defp guarded_remove(workspace, host, recorded, run_hook? \\ true) do
    settings = Config.settings!()
    root = if is_nil(host), do: Config.local_workspace_root(), else: settings.workspace.root
    hook = if run_hook?, do: settings.hooks.before_remove || "", else: ""
    timeout = settings.hooks.timeout_ms
    nonce = Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)

    with :ok <- validate_removal_input(workspace),
         :ok <- validate_removal_input(root),
         {:ok, {output, status}} <- run_remove_helper(workspace, root, hook, timeout, recorded, nonce, host) do
      removal_result(output, status, workspace, host, nonce)
    else
      {:error, reason} -> {:error, reason, ""}
    end
  end

  defp validate_removal_input(path) when is_binary(path) do
    cond do
      String.trim(path) == "" -> {:error, {:workspace_path_unreadable, path, :empty}}
      String.contains?(path, ["\n", "\r", <<0>>]) -> {:error, {:workspace_path_unreadable, path, :invalid_characters}}
      true -> :ok
    end
  end

  defp validate_removal_input(path), do: {:error, {:workspace_path_unreadable, path, :invalid}}

  defp run_remove_helper(workspace, root, hook, timeout, recorded, nonce, nil) do
    case System.find_executable("python3") do
      nil ->
        {:error, {:workspace_remove_unavailable, :python3_not_found}}

      python ->
        args = ["-c", @remove_script, workspace, root, hook, to_string(timeout), to_string(recorded), nonce]
        port = Port.open({:spawn_executable, String.to_charlist(python)}, [:binary, :exit_status, :stderr_to_stdout, args: Enum.map(args, &String.to_charlist/1)])
        collect_removal_output(port, "", System.monotonic_time(:millisecond) + timeout + 6_000)
    end
  rescue
    _error in [ArgumentError, ErlangError] -> {:error, {:workspace_remove_unavailable, :python3_spawn_failed}}
  end

  defp run_remove_helper(workspace, root, hook, timeout, recorded, nonce, host) when is_binary(host) do
    missing = @remove_marker <> Jason.encode!(%{status: "error", reason: "python3_not_found", nonce: nonce})

    script =
      [
        "removal_nonce=#{nonce}",
        remote_shell_assign("workspace", workspace),
        remote_shell_assign("workspace_root", root),
        "command -v python3 >/dev/null 2>&1 || { printf '%s\\n' #{shell_escape(missing)}; exit 1; }",
        "python3 -c #{shell_escape(@remove_script)} \"$workspace\" \"$workspace_root\" #{shell_escape(hook)} #{timeout} #{recorded} #{nonce}"
      ]
      |> Enum.join("\n")

    with {:ok, port} <- SSH.start_port(host, script) do
      collect_removal_output(port, "", System.monotonic_time(:millisecond) + timeout + 6_000)
    end
  rescue
    _error in [ArgumentError, ErlangError] -> {:error, {:workspace_remove_unavailable, :ssh_spawn_failed}}
  end

  defp collect_removal_output(port, output, deadline) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {^port, {:data, data}} ->
        if byte_size(output) + byte_size(data) <= 65_536 do
          collect_removal_output(port, output <> data, deadline)
        else
          close_removal_port(port)
          {:error, {:workspace_remove_failed, :output_limit}}
        end

      {^port, {:exit_status, status}} ->
        {:ok, {output, status}}
    after
      remaining ->
        close_removal_port(port)
        {:error, {:workspace_remove_failed, :timeout}}
    end
  end

  defp close_removal_port(port) do
    Port.close(port)
  rescue
    ArgumentError -> :ok
  end

  defp removal_result(output, status, workspace, host, nonce) do
    results =
      if byte_size(output) <= 65_536 do
        Enum.flat_map(String.split(output, "\n"), fn line ->
          if String.starts_with?(line, @remove_marker) do
            case Jason.decode(String.replace_prefix(line, @remove_marker, "")) do
              {:ok, %{"nonce" => ^nonce} = result} -> [result]
              _ -> []
            end
          else
            []
          end
        end)
      else
        []
      end

    case results do
      [%{"status" => "ok"} = result] when status == 0 ->
        log_removal_hook(result["hook"], workspace, host)
        {:ok, []}

      [%{"status" => "error"} = result] ->
        {:error, removal_error(result, workspace, host), ""}

      _ ->
        {:error, {:workspace_remove_failed, host, status, :invalid_result}, ""}
    end
  end

  defp removal_error(%{"reason" => reason}, _workspace, _host) when reason in ["python3_not_found", "unsupported_python"],
    do: {:workspace_remove_unavailable, if(reason == "python3_not_found", do: :python3_not_found, else: :python3_11_descriptor_support_required)}

  defp removal_error(%{"reason" => "equals_root", "workspace" => path, "root" => root}, _workspace, _host), do: {:workspace_equals_root, path, root}
  defp removal_error(%{"reason" => "outside_root", "workspace" => path, "root" => root}, _workspace, _host), do: {:workspace_outside_root, path, root}
  defp removal_error(%{"reason" => "symlink_escape", "workspace" => path, "root" => root}, _workspace, _host), do: {:workspace_symlink_escape, path, root}
  defp removal_error(result, workspace, host), do: {:workspace_remove_failed, host, result["reason"], result["errno"], workspace}

  defp log_removal_hook(%{"status" => "failed", "exit_status" => status}, workspace, host),
    do: Logger.warning("Workspace hook failed hook=before_remove #{removal_context(workspace)} workspace=#{workspace} worker_host=#{worker_host_for_log(host)} status=#{status}")

  defp log_removal_hook(%{"status" => "timeout"}, workspace, host),
    do: Logger.warning("Workspace hook timed out hook=before_remove #{removal_context(workspace)} workspace=#{workspace} worker_host=#{worker_host_for_log(host)}")

  defp log_removal_hook(_hook, _workspace, _host), do: :ok

  defp removal_context(workspace), do: issue_log_context(%{issue_id: nil, issue_identifier: Path.basename(workspace)})

  @spec run_before_run_hook(Path.t(), map() | String.t() | nil, worker_host()) ::
          :ok | {:error, term()}
  def run_before_run_hook(workspace, issue_or_identifier, worker_host \\ nil) when is_binary(workspace) do
    issue_context = issue_context(issue_or_identifier)
    hooks = Config.settings!().hooks

    case hooks.before_run do
      nil ->
        :ok

      command ->
        run_hook(command, workspace, issue_context, "before_run", worker_host)
    end
  end

  @spec run_after_run_hook(Path.t(), map() | String.t() | nil, worker_host()) :: :ok
  def run_after_run_hook(workspace, issue_or_identifier, worker_host \\ nil) when is_binary(workspace) do
    issue_context = issue_context(issue_or_identifier)
    hooks = Config.settings!().hooks

    case hooks.after_run do
      nil ->
        :ok

      command ->
        run_hook(command, workspace, issue_context, "after_run", worker_host)
        |> ignore_hook_failure()
    end
  end

  defp workspace_path_for_issue(safe_id, nil) when is_binary(safe_id) do
    Config.local_workspace_root()
    |> Path.join(safe_id)
    |> PathSafety.canonicalize()
  end

  defp workspace_path_for_issue(safe_id, worker_host) when is_binary(safe_id) and is_binary(worker_host) do
    {:ok, Path.join(Config.settings!().workspace.root, safe_id)}
  end

  @doc """
  Returns the collision-safe directory name for an issue identifier.

  The hash is derived from the original identifier so callers that only know the identifier can
  derive the same key as callers holding a full tracker issue.
  """
  @spec workspace_key(map() | String.t() | nil) :: String.t()
  def workspace_key(%{identifier: identifier}), do: workspace_key(identifier)

  def workspace_key(identifier) when is_binary(identifier) do
    safe_identifier = safe_identifier(identifier)

    if safe_identifier == identifier do
      safe_identifier
    else
      "#{safe_identifier}--#{short_identifier_hash(identifier)}"
    end
  end

  def workspace_key(_identifier), do: "issue"

  defp safe_identifier(identifier) when is_binary(identifier),
    do: String.replace(identifier, ~r/[^a-zA-Z0-9._-]/, "_")

  defp short_identifier_hash(identifier) do
    :crypto.hash(:sha256, identifier)
    |> Base.encode16(case: :lower)
    |> binary_part(0, 16)
  end

  defp maybe_run_after_create_hook(workspace, issue_context, created?, worker_host) do
    hooks = Config.settings!().hooks

    case created? do
      true ->
        case hooks.after_create do
          nil ->
            :ok

          command ->
            run_hook(command, workspace, issue_context, "after_create", worker_host)
        end

      false ->
        :ok
    end
  end

  defp cleanup_failed_new_workspace(_workspace, false, _worker_host), do: :ok

  defp cleanup_failed_new_workspace(workspace, true, host) do
    case guarded_remove(workspace, host, false, false) do
      {:ok, _} -> :ok
      {:error, reason, _path} -> Logger.warning("Failed to remove partial workspace worker_host=#{worker_host_for_log(host)} reason=#{inspect(reason)}")
    end
  end

  defp ignore_hook_failure(:ok), do: :ok
  defp ignore_hook_failure({:error, _reason}), do: :ok

  defp run_hook(command, workspace, issue_context, hook_name, nil) do
    timeout_ms = Config.settings!().hooks.timeout_ms

    Logger.info("Running workspace hook hook=#{hook_name} #{issue_log_context(issue_context)} workspace=#{workspace} worker_host=local")

    task =
      Task.async(fn ->
        System.cmd("sh", ["-lc", command], cd: workspace, stderr_to_stdout: true)
      end)

    case Task.yield(task, timeout_ms) do
      {:ok, cmd_result} ->
        handle_hook_command_result(cmd_result, workspace, issue_context, hook_name)

      nil ->
        Task.shutdown(task, :brutal_kill)

        Logger.warning("Workspace hook timed out hook=#{hook_name} #{issue_log_context(issue_context)} workspace=#{workspace} worker_host=local timeout_ms=#{timeout_ms}")

        {:error, {:workspace_hook_timeout, hook_name, timeout_ms}}
    end
  end

  defp run_hook(command, workspace, issue_context, hook_name, worker_host) when is_binary(worker_host) do
    timeout_ms = Config.settings!().hooks.timeout_ms

    Logger.info("Running workspace hook hook=#{hook_name} #{issue_log_context(issue_context)} workspace=#{workspace} worker_host=#{worker_host}")

    case run_remote_command(worker_host, "cd #{shell_escape(workspace)} && #{command}", timeout_ms) do
      {:ok, cmd_result} ->
        handle_hook_command_result(cmd_result, workspace, issue_context, hook_name)

      {:error, {:workspace_hook_timeout, ^hook_name, _timeout_ms} = reason} ->
        {:error, reason}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp handle_hook_command_result({_output, 0}, _workspace, _issue_id, _hook_name) do
    :ok
  end

  defp handle_hook_command_result({output, status}, workspace, issue_context, hook_name) do
    sanitized_output = sanitize_hook_output_for_log(output)

    Logger.warning("Workspace hook failed hook=#{hook_name} #{issue_log_context(issue_context)} workspace=#{workspace} status=#{status} output=#{inspect(sanitized_output)}")

    {:error, {:workspace_hook_failed, hook_name, status, output}}
  end

  defp sanitize_hook_output_for_log(output, max_bytes \\ 2_048) do
    binary_output = IO.iodata_to_binary(output)

    case byte_size(binary_output) <= max_bytes do
      true ->
        binary_output

      false ->
        binary_part(binary_output, 0, max_bytes) <> "... (truncated)"
    end
  end

  defp validate_workspace_path(workspace, nil) when is_binary(workspace) do
    validate_local_workspace_path(workspace, Config.local_workspace_root())
  end

  defp validate_workspace_path(workspace, worker_host)
       when is_binary(workspace) and is_binary(worker_host) do
    cond do
      String.trim(workspace) == "" ->
        {:error, {:workspace_path_unreadable, workspace, :empty}}

      String.contains?(workspace, ["\n", "\r", <<0>>]) ->
        {:error, {:workspace_path_unreadable, workspace, :invalid_characters}}

      true ->
        :ok
    end
  end

  defp validate_local_workspace_path(workspace, workspace_root)
       when is_binary(workspace) and is_binary(workspace_root) do
    expanded_workspace = Path.expand(workspace)
    expanded_root = Path.expand(workspace_root)
    expanded_root_prefix = expanded_root <> "/"

    with {:ok, canonical_workspace} <- PathSafety.canonicalize(expanded_workspace),
         {:ok, canonical_root} <- PathSafety.canonicalize(expanded_root) do
      canonical_root_prefix = canonical_root <> "/"

      cond do
        canonical_workspace == canonical_root ->
          {:error, {:workspace_equals_root, canonical_workspace, canonical_root}}

        String.starts_with?(canonical_workspace <> "/", canonical_root_prefix) ->
          :ok

        String.starts_with?(expanded_workspace <> "/", expanded_root_prefix) ->
          {:error, {:workspace_symlink_escape, expanded_workspace, canonical_root}}

        true ->
          {:error, {:workspace_outside_root, canonical_workspace, canonical_root}}
      end
    else
      {:error, {:path_canonicalize_failed, path, reason}} ->
        {:error, {:workspace_path_unreadable, path, reason}}
    end
  end

  defp remote_shell_assign(variable_name, raw_path)
       when is_binary(variable_name) and is_binary(raw_path) do
    [
      "#{variable_name}=#{shell_escape(raw_path)}",
      "case \"$#{variable_name}\" in",
      "  '~') #{variable_name}=\"$HOME\" ;;",
      "  '~/'*) " <> variable_name <> "=\"$HOME/${" <> variable_name <> "#\\~/}\" ;;",
      "esac"
    ]
    |> Enum.join("\n")
  end

  defp parse_remote_workspace_output(output) do
    lines = String.split(IO.iodata_to_binary(output), "\n", trim: true)

    payload =
      Enum.find_value(lines, fn line ->
        case String.split(line, "\t", parts: 3) do
          [@remote_workspace_marker, created, path] when created in ["0", "1"] and path != "" ->
            {created == "1", path}

          _ ->
            nil
        end
      end)

    case payload do
      {created?, workspace} when is_boolean(created?) and is_binary(workspace) ->
        {:ok, workspace, created?}

      _ ->
        {:error, {:workspace_prepare_failed, :invalid_output, output}}
    end
  end

  defp run_remote_command(worker_host, script, timeout_ms)
       when is_binary(worker_host) and is_binary(script) and is_integer(timeout_ms) and timeout_ms > 0 do
    task =
      Task.async(fn ->
        SSH.run(worker_host, script, stderr_to_stdout: true)
      end)

    case Task.yield(task, timeout_ms) do
      {:ok, result} ->
        result

      nil ->
        Task.shutdown(task, :brutal_kill)
        {:error, {:workspace_hook_timeout, "remote_command", timeout_ms}}
    end
  end

  defp shell_escape(value) when is_binary(value) do
    "'" <> String.replace(value, "'", "'\"'\"'") <> "'"
  end

  defp worker_host_for_log(nil), do: "local"
  defp worker_host_for_log(worker_host), do: worker_host

  defp issue_context(%{id: issue_id, identifier: identifier}) do
    %{
      issue_id: issue_id,
      issue_identifier: identifier || "issue"
    }
  end

  defp issue_context(identifier) when is_binary(identifier) do
    %{
      issue_id: nil,
      issue_identifier: identifier
    }
  end

  defp issue_context(_identifier) do
    %{
      issue_id: nil,
      issue_identifier: "issue"
    }
  end

  defp issue_log_context(%{issue_id: issue_id, issue_identifier: issue_identifier}) do
    "issue_id=#{issue_id || "n/a"} issue_identifier=#{issue_identifier || "issue"}"
  end
end
