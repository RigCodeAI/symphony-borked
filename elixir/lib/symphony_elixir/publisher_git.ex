defmodule SymphonyElixir.PublisherGit do
  @moduledoc false

  alias SymphonyElixir.CandidateGit

  @remote_url "https://github.com/RigCodeAI/rig.git"
  @command_timeout_ms 30_000
  @max_command_output 16_384
  @process_group_kill_grace_ms 1_000

  @spec push(Path.t(), String.t(), String.t(), String.t(), String.t() | nil) :: :ok | {:error, term()}
  def push(workspace, sha, branch, token, expected_remote_sha)
      when is_binary(workspace) and is_binary(sha) and is_binary(branch) and is_binary(token) do
    with :ok <- validate_sha(sha),
         :ok <- validate_expected_sha(expected_remote_sha),
         :ok <- validate_branch(branch),
         :ok <- validate_token(token),
         {:ok, git} <- git_executable() do
      CandidateGit.with_snapshot(workspace, fn git_dir ->
        push_from_snapshot(git, git_dir, workspace, sha, branch, token, expected_remote_sha, &run_bounded/4)
      end)
    end
  end

  def push(_workspace, _sha, _branch, _token, _expected_remote_sha), do: {:error, :invalid_publisher_git_arguments}

  @doc false
  @spec push_for_test(Path.t(), String.t(), String.t(), String.t(), String.t() | nil, function()) ::
          :ok | {:error, term()}
  def push_for_test(workspace, sha, branch, token, expected_remote_sha, command_fun)
      when is_binary(workspace) and is_binary(sha) and is_binary(branch) and is_binary(token) and
             is_function(command_fun, 4) do
    with :ok <- validate_sha(sha),
         :ok <- validate_expected_sha(expected_remote_sha),
         :ok <- validate_branch(branch),
         :ok <- validate_token(token),
         {:ok, git} <- git_executable() do
      CandidateGit.with_snapshot(workspace, fn git_dir ->
        push_from_snapshot(git, git_dir, workspace, sha, branch, token, expected_remote_sha, command_fun)
      end)
    end
  end

  def push_for_test(_workspace, _sha, _branch, _token, _expected_remote_sha, _command_fun),
    do: {:error, :invalid_publisher_git_arguments}

  defp push_from_snapshot(git, git_dir, workspace, sha, branch, token, expected_remote_sha, command_fun) do
    with {:ok, resolved_sha} <-
           command_fun.(git, git_args(git_dir, workspace, ["rev-parse", "--verify", "--end-of-options", sha <> "^{commit}"]), workspace, git_environment()),
         true <- resolved_sha == sha or {:error, :publisher_commit_mismatch},
         {:ok, snapshot_head} <-
           command_fun.(git, git_args(git_dir, workspace, ["rev-parse", "--verify", "--end-of-options", "HEAD^{commit}"]), workspace, git_environment()),
         true <- snapshot_head == sha or {:error, :candidate_head_changed},
         :ok <-
           push_commit(
             command_fun,
             git,
             git_dir,
             workspace,
             sha,
             branch,
             token,
             expected_remote_sha
           ) do
      :ok
    else
      false -> {:error, :publisher_commit_mismatch}
      {:error, :candidate_head_changed} -> {:error, :candidate_head_changed}
      {:error, _reason} = error -> error
      _ -> {:error, :publisher_git_failed}
    end
  end

  defp push_commit(command_fun, git, git_dir, workspace, sha, branch, token, expected_remote_sha) do
    ref = "refs/heads/" <> branch
    lease = "--force-with-lease=" <> ref <> ":" <> (expected_remote_sha || "")

    args =
      git_args(git_dir, workspace, [
        "-c",
        "protocol.https.allow=always",
        "-c",
        "http.followRedirects=false",
        "push",
        "--porcelain",
        "--no-verify",
        lease,
        @remote_url,
        sha <> ":" <> ref
      ])

    case command_fun.(git, args, workspace, git_environment(token)) do
      {:ok, _output} -> :ok
      {:error, reason} -> {:error, {:git_push_unknown, safe_push_error(reason)}}
      _ -> {:error, {:git_push_unknown, :command_failed}}
    end
  end

  defp git_args(git_dir, workspace, args) do
    [
      "--no-replace-objects",
      "--no-pager",
      "--no-optional-locks",
      "--git-dir=" <> git_dir,
      "--work-tree=" <> workspace
      | args
    ]
  end

  defp run_bounded(git, args, cwd, env) do
    with timeout when is_binary(timeout) <- System.find_executable("timeout") do
      shell_args = [
        "-c",
        "read -r start; exec \"$@\"",
        "publisher",
        timeout,
        "--signal=TERM",
        "--kill-after=#{div(@process_group_kill_grace_ms, 1_000)}s",
        "#{div(@command_timeout_ms, 1_000)}s",
        git | args
      ]

      options = [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        :hide,
        {:args, Enum.map(shell_args, &String.to_charlist/1)},
        {:cd, String.to_charlist(cwd)},
        {:env, port_environment(env)}
      ]

      try do
        port = Port.open({:spawn_executable, ~c"/bin/sh"}, options)
        {:os_pid, group_id} = Port.info(port, :os_pid)
        Port.command(port, "start\n")
        deadline = System.monotonic_time(:millisecond) + @command_timeout_ms + @process_group_kill_grace_ms + 5_000
        collect_port_output(port, group_id, <<>>, deadline)
      rescue
        _error -> {:error, :git_execution_failed}
      catch
        _kind, _reason -> {:error, :git_execution_failed}
      end
    else
      nil -> {:error, :git_timeout_unavailable}
    end
  end

  defp collect_port_output(port, group_id, output, deadline) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {^port, {:data, data}} ->
        next_output = output <> data

        if byte_size(next_output) > @max_command_output do
          kill_process_group(group_id)
          close_port(port)
          {:error, :git_output_limit_exceeded}
        else
          collect_port_output(port, group_id, next_output, deadline)
        end

      {^port, {:exit_status, status}} ->
        kill_process_group(group_id)

        cond do
          status == 0 -> {:ok, trim_newlines(output)}
          status in [124, 137] -> {:error, :git_command_timeout}
          true -> {:error, :git_command_failed}
        end
    after
      remaining ->
        kill_process_group(group_id)
        close_port(port)
        {:error, :git_command_timeout}
    end
  end

  defp kill_process_group(group_id) do
    _ = System.cmd("kill", ["-KILL", "--", "-#{group_id}"], stderr_to_stdout: true)
    :ok
  rescue
    _error -> :ok
  catch
    _kind, _reason -> :ok
  end

  defp close_port(port) do
    try do
      Port.close(port)
    rescue
      _error -> :ok
    catch
      _kind, _reason -> :ok
    end
  end

  defp port_environment(env) do
    inherited_keys = System.get_env() |> Map.keys() |> Enum.uniq()
    clear = Enum.map(inherited_keys, &{String.to_charlist(&1), nil})
    configured = Enum.map(env, fn {key, value} -> {String.to_charlist(key), String.to_charlist(value)} end)
    clear ++ configured
  end

  defp git_environment(token \\ nil) do
    inherited =
      System.get_env()
      |> Enum.filter(fn {name, _value} -> String.upcase(name) in ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "SSL_CERT_FILE", "SSL_CERT_DIR"] end)

    fixed = [
      {"PATH", System.get_env("PATH") || "/usr/bin:/bin"},
      {"HOME", "/dev/null"},
      {"GIT_CONFIG_COUNT", if(is_binary(token), do: "1", else: "0")},
      {"GIT_CONFIG_GLOBAL", "/dev/null"},
      {"GIT_CONFIG_SYSTEM", "/dev/null"},
      {"GIT_CONFIG_NOSYSTEM", "1"},
      {"GIT_ATTR_NOSYSTEM", "1"},
      {"GIT_OPTIONAL_LOCKS", "0"},
      {"GIT_TERMINAL_PROMPT", "0"},
      {"GIT_PAGER", "cat"},
      {"GIT_NO_REPLACE_OBJECTS", "1"},
      {"GIT_NO_LAZY_FETCH", "1"},
      {"GIT_TRACE", "0"},
      {"GIT_TRACE_CURL", "0"},
      {"GIT_CURL_VERBOSE", "0"}
    ]

    auth =
      if is_binary(token) do
        encoded = Base.encode64("x-access-token:" <> token)
        [{"GIT_CONFIG_KEY_0", "http.https://github.com/.extraheader"}, {"GIT_CONFIG_VALUE_0", "AUTHORIZATION: basic " <> encoded}]
      else
        []
      end

    inherited ++ fixed ++ auth
  end

  defp git_executable do
    case System.find_executable("git") do
      path when is_binary(path) -> {:ok, path}
      nil -> {:error, :git_executable_missing}
    end
  end

  defp validate_sha(sha) when is_binary(sha) and byte_size(sha) in [40, 64] do
    if lowercase_hex?(sha), do: :ok, else: {:error, :invalid_publisher_commit_sha}
  end

  defp validate_sha(_sha), do: {:error, :invalid_publisher_commit_sha}

  defp validate_expected_sha(nil), do: :ok
  defp validate_expected_sha(sha), do: validate_sha(sha)

  defp validate_branch(branch) when is_binary(branch) and byte_size(branch) in 1..240 do
    ref = "refs/heads/" <> branch

    if String.match?(branch, ~r/^[A-Za-z0-9][A-Za-z0-9._\/-]*$/) and
         not String.contains?(branch, ["..", "//", "@{", "\\"]) and
         not String.ends_with?(branch, ["/", ".", ".lock"]) and
         not Enum.any?(String.split(branch, "/"), &String.starts_with?(&1, ".")) and
         String.valid?(ref) do
      :ok
    else
      {:error, :invalid_publisher_branch}
    end
  end

  defp validate_branch(_branch), do: {:error, :invalid_publisher_branch}

  defp validate_token(token) when is_binary(token) do
    if String.trim(token) != "" and not String.contains?(token, ["\r", "\n", "\0"]),
      do: :ok,
      else: {:error, :invalid_publisher_token}
  end

  defp safe_push_error(reason) when reason in [:git_command_timeout, :git_output_limit_exceeded, :git_execution_failed, :git_timeout_unavailable, :git_command_failed],
    do: reason

  defp safe_push_error(_reason), do: :command_failed

  defp lowercase_hex?(value) do
    for <<byte <- value>>, reduce: true do
      valid? -> valid? and (byte in ?0..?9 or byte in ?a..?f)
    end
  end

  defp trim_newlines(output), do: trim_newlines(output, byte_size(output))
  defp trim_newlines(output, 0), do: output

  defp trim_newlines(output, size) do
    case :binary.at(output, size - 1) do
      byte when byte in [?\n, ?\r] -> trim_newlines(binary_part(output, 0, size - 1), size - 1)
      _byte -> output
    end
  end
end
