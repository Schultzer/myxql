defmodule MyXQL.MixProject do
  use Mix.Project

  def project() do
    [
      app: :myxql,
      version: "0.1.0",
      elixir: "~> 1.4",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application() do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps() do
    [
      {:db_connection, github: "elixir-ecto/db_connection"},
      {:decimal, "~> 1.5"},
      {:binpp, ">= 0.0.0", only: [:dev, :test]}
    ]
  end
end
