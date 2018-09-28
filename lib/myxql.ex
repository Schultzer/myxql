defmodule MyXQL do
  def start_link(opts) do
    DBConnection.start_link(MyXQL.Protocol, opts)
  end

  def query(conn, statement, params \\ [], opts \\ []) do
    case prepare_execute(conn, "", statement, params, opts) do
      {:ok, _query, result} ->
        {:ok, result}

      {:error, exception} ->
        {:error, exception}
    end
  end

  def query!(conn, statement, params \\ [], opts \\ []) do
    case query(conn, statement, params, opts) do
      {:ok, result} -> result
      {:error, exception} -> raise exception
    end
  end

  # TODO: handle query names
  def prepare(conn, _name, statement, opts \\ []) do
    query = %MyXQL.Query{statement: statement}
    DBConnection.prepare(conn, query, opts)
  end

  # TODO: handle query names
  def prepare_execute(conn, _name, statement, params \\ [], opts \\ []) do
    query = %MyXQL.Query{statement: statement}

    case DBConnection.prepare_execute(conn, query, params, opts) do
      {:ok, query, result} ->
        {:ok, query, result}

      {:error, exception} ->
        {:error, exception}
    end
  end

  defdelegate execute(conn, query, params \\ [], opts \\ []), to: DBConnection

  defdelegate transaction(conn, fun, opts \\ []), to: DBConnection

  defdelegate rollback(conn, reason), to: DBConnection

  def child_spec(opts) do
    DBConnection.child_spec(MyXQL.Protocol, opts)
  end
end
