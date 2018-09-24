defmodule MyXQL.Protocol do
  @moduledoc false
  use DBConnection
  import MyXQL.Messages
  alias MyXQL.{Error, Query, Result}

  @impl true
  def connect(opts) do
    hostname = Keyword.fetch!(opts, :hostname)
    port = Keyword.fetch!(opts, :port)
    username = Keyword.fetch!(opts, :username)
    password = Keyword.fetch!(opts, :password)
    database = Keyword.fetch!(opts, :database)
    timeout = Keyword.fetch!(opts, :timeout)
    socket_opts = [:binary, active: false]

    case :gen_tcp.connect(String.to_charlist(hostname), port, socket_opts, timeout) do
      {:ok, sock} ->
        handshake(sock, username, password, database)

      {:error, reason} ->
        message = reason |> :inet.format_error() |> List.to_string()
        {:error, %MyXQL.Error{message: message}}
    end
  end

  # TODO: wip
  def query(conn, statement) do
    data = encode_com_query(statement)
    :ok = :gen_tcp.send(conn.sock, data)
    {:ok, data} = :gen_tcp.recv(conn.sock, 0)

    case decode_com_query_response(data) do
      ok_packet(last_insert_id: last_insert_id) ->
        {:ok, %MyXQL.Result{last_insert_id: last_insert_id}}

      resultset(column_definitions: column_definitions, rows: rows) ->
        columns = Enum.map(column_definitions, &elem(&1, 1))
        {:ok, %MyXQL.Result{columns: columns, rows: rows}}

      err_packet(error_message: message) ->
        {:error, %MyXQL.Error{message: message}}
    end
  end

  @impl true
  def disconnect(_, _), do: raise "not implemented yet"

  @impl true
  def checkout(state) do
    {:ok, state}
  end

  @impl true
  def checkin(state) do
    {:ok, state}
  end

  @impl true
  def handle_prepare(%Query{} = query, _opts, state) do
    data = encode_com_stmt_prepare(query.statement)
    :ok = :gen_tcp.send(state.sock, data)
    {:ok, data} = :gen_tcp.recv(state.sock, 0)

    case decode_com_stmt_prepare_response(data) do
      com_stmt_prepare_ok(statement_id: statement_id) ->
        {:ok, %{query | statement_id: statement_id}, state}

      err_packet(error_message: error_message) ->
        exception = %Error{message: error_message, query: query}
        {:error, exception, state}
    end
  end

  @impl true
  def handle_execute(%Query{} = query, params, _opts, state) do
    data = encode_com_stmt_execute(query.statement_id, params)
    :ok = :gen_tcp.send(state.sock, data)
    {:ok, data} = :gen_tcp.recv(state.sock, 0)

    case decode_com_stmt_execute_response(data) do
      resultset(column_definitions: column_definitions, rows: rows) ->
        columns = Enum.map(column_definitions, &elem(&1, 1))
        result = %Result{columns: columns, num_rows: length(rows), rows: rows}
        {:ok, query, result, state}

      ok_packet(status_flags: _status_flags, affected_rows: affected_rows, last_insert_id: last_insert_id) ->
        result = %Result{columns: [], rows: [], num_rows: affected_rows, last_insert_id: last_insert_id}
        {:ok, query, result, state}

      err_packet(error_message: error_message) ->
        exception = %Error{message: error_message, query: query}
        {:error, exception, state}
    end
  end

  @impl true
  def handle_close(_query, _opts, state) do
    # TODO: https://dev.mysql.com/doc/internals/en/com-stmt-close.html
    # TODO: return %MyXQL.Result{}
    result = nil
    {:ok, result, state}
  end

  @impl true
  def ping(state) do
    # TODO: https://dev.mysql.com/doc/internals/en/com-ping.html
    {:ok, state}
  end

  @impl true
  def handle_begin(_, _), do: raise "not implemented yet"

  @impl true
  def handle_commit(_, _), do: raise "not implemented yet"

  @impl true
  def handle_rollback(_, _), do: raise "not implemented yet"

  @impl true
  def handle_status(_, _), do: raise "not implemented yet"

  @impl true
  def handle_declare(_, _, _, _), do: raise "not implemented yet"

  @impl true
  def handle_fetch(_, _, _, _), do: raise "not implemented yet"

  @impl true
  def handle_deallocate(_, _, _, _), do: raise "not implemented yet"

  ## Internals

  defp handshake(sock, username, password, database) do
    {:ok, data} = :gen_tcp.recv(sock, 0)

    handshake_v10(
      auth_plugin_name: auth_plugin_name,
      auth_plugin_data1: auth_plugin_data1,
      auth_plugin_data2: auth_plugin_data2
    ) = MyXQL.Messages.decode_handshake_v10(data)

    # TODO: MySQL 8.0 defaults to "caching_sha2_password", which we don't support yet,
    #       and will send AuthSwitchRequest which we'll need to handle.
    #       https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthSwitchRequest
    "mysql_native_password" = auth_plugin_name

    auth_plugin_data = <<auth_plugin_data1::binary, auth_plugin_data2::binary>>
    auth_response = if password, do: MyXQL.Utils.mysql_native_password(password, auth_plugin_data)

    data = MyXQL.Messages.encode_handshake_response_41(username, auth_response, database)
    :ok = :gen_tcp.send(sock, data)
    {:ok, data} = :gen_tcp.recv(sock, 0)

    case decode_response_packet(data) do
      ok_packet(warnings: 0) ->
        {:ok, %{sock: sock}}

      err_packet(error_message: message) ->
        {:error, %MyXQL.Error{message: message}}
    end
  end
end
