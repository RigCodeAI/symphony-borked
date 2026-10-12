defmodule Mix.Tasks.Workspace.BeforeRemove do
  use Mix.Task

  @shortdoc "Compatibility hook that leaves GitHub pull requests unchanged"

  @moduledoc """
  Leaves GitHub pull requests unchanged before workspace removal.

  This compatibility task succeeds without running Git or GitHub commands.
  Existing `before_remove` hooks can keep invoking it. The `--branch` and
  `--repo` options remain accepted but have no effect.

  This task does not authorize workspace deletion or verify merge, archival,
  or cancellation recovery. Those checks belong to the cleanup owner.

  Usage:

      mix workspace.before_remove
      mix workspace.before_remove --branch feature/my-branch
      mix workspace.before_remove --repo RigCodeAI/rig
  """

  @impl Mix.Task
  def run(args) do
    {opts, _argv, invalid} =
      OptionParser.parse(args,
        strict: [branch: :string, help: :boolean, repo: :string],
        aliases: [h: :help]
      )

    cond do
      opts[:help] ->
        Mix.shell().info(@moduledoc)

      invalid != [] ->
        Mix.raise("Invalid option(s): #{inspect(invalid)}")

      true ->
        :ok
    end
  end
end
