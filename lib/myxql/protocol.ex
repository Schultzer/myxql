defmodule MyXQL.Protocol do
  @moduledoc false
  use DBConnection
  use Bitwise
  import MyXQL.Messages
  alias MyXQL.{Error, Query, Result}

  defstruct [
    :sock,
    :connection_id,
    :transaction_status
  ]

  @impl true
  def connect(opts) do
    hostname = Keyword.fetch!(opts, :hostname)
    port = Keyword.fetch!(opts, :port)
    username = Keyword.fetch!(opts, :username)
    password = Keyword.fetch!(opts, :password)
    database = Keyword.fetch!(opts, :database)
    timeout = Keyword.fetch!(opts, :timeout)
    socket_opts = [:binary, active: false]
    skip_database? = Keyword.get(opts, :skip_database, false)

    case :gen_tcp.connect(String.to_charlist(hostname), port, socket_opts, timeout) do
      {:ok, sock} ->
        handshake(sock, username, password, database, skip_database?)

      {:error, reason} ->
        message = reason |> :inet.format_error() |> List.to_string()
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
  def handle_prepare(%Query{} = query, _opts, s) do
    # IO.inspect {:prepare, query.name, query.statement, query.ref, s.connection_id}

    data = encode_com_stmt_prepare(query.statement)
    :ok = :gen_tcp.send(s.sock, data)
    {:ok, data} = :gen_tcp.recv(s.sock, 0)

    case decode_com_stmt_prepare_response(data) do
      com_stmt_prepare_ok(statement_id: statement_id) ->
        {:ok, %{query | statement_id: statement_id}, s}

      err_packet(error_message: error_message) ->
        exception = %Error{message: error_message, query: query}
        {:error, exception, s}
    end
  end

  @impl true
  def handle_execute(%Query{} = query, params, _opts, s) do
    # IO.inspect {:execute, query.name, query.statement, query.ref, s.connection_id}

    data = encode_com_stmt_execute(query.statement_id, params)
    :ok = :gen_tcp.send(s.sock, data)
    {:ok, data} = :gen_tcp.recv(s.sock, 0)

    case decode_com_stmt_execute_response(data) do
      resultset(column_definitions: column_definitions, rows: rows, status_flags: status_flags) ->
        columns = Enum.map(column_definitions, &elem(&1, 1))
        result = %Result{columns: columns, num_rows: length(rows), rows: rows}
        {:ok, query, result, update_status(s, status_flags)}

      ok_packet(status_flags: status_flags, affected_rows: affected_rows, last_insert_id: last_insert_id) ->
        result = %Result{columns: [], rows: nil, num_rows: affected_rows, last_insert_id: last_insert_id}
        {:ok, query, result, update_status(s, status_flags)}

      err_packet(error_message: error_message) ->
        exception = %Error{message: error_message, query: query}
        {:error, exception, s}
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
  def handle_begin(opts, s) do
    statement = "BEGIN"
    handle_transaction(statement, opts, s)
  end

  @impl true
  def handle_commit(opts, s) do
    statement = "COMMIT"
    handle_transaction(statement, opts, s)
  end

  @impl true
  def handle_rollback(opts, s) do
    statement = "ROLLBACK"
    handle_transaction(statement, opts, s)
  end

  @impl true
  def handle_status(_opts, s) do
    {s.transaction_status, s}
  end

  @impl true
  def handle_declare(_, _, _, _), do: raise "not implemented yet"

  @impl true
  def handle_fetch(_, _, _, _), do: raise "not implemented yet"

  @impl true
  def handle_deallocate(_, _, _, _), do: raise "not implemented yet"

  ## Internals

  defp handshake(sock, username, password, database, skip_database?) do
    {:ok, data} = :gen_tcp.recv(sock, 0)

    handshake_v10(
      conn_id: connection_id,
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

    data = MyXQL.Messages.encode_handshake_response_41(username, auth_response, database, skip_database?)
    :ok = :gen_tcp.send(sock, data)
    {:ok, data} = :gen_tcp.recv(sock, 0)

    case decode_response_packet(data) do
      ok_packet(warning_count: 0) ->
        {:ok, %__MODULE__{sock: sock, connection_id: connection_id, transaction_status: :idle}}

      err_packet(error_message: message) ->
        {:error, %MyXQL.Error{message: message}}
    end
  end

  defp handle_transaction(statement, _opts, s) do
    case send_text_query(s, statement) do
      ok_packet(status_flags: status_flags) ->
        {:ok, :foo, update_status(s, status_flags)}
    end
  end

  defp send_text_query(s, statement) do
    data = encode_com_query(statement)
    :ok = :gen_tcp.send(s.sock, data)
    {:ok, data} = :gen_tcp.recv(s.sock, 0)
    decode_com_query_response(data)
  end

  # https://dev.mysql.com/doc/internals/en/status-flags.html
  defp transaction_status(status_flags) do
    if (status_flags &&& 0x0001) == 0x0001 do
      :transaction
    else
      :idle
    end
  end

  defp update_status(s, status_flags) do
    %{s | transaction_status: transaction_status(status_flags)}
  end
end
