defmodule MyXQLTest do
  use ExUnit.Case, async: true

  @opts [
    hostname: "127.0.0.1",
    port: 8006,
    username: "root",
    password: "secret",
    database: "myxql_test",
    timeout: 5000
  ]

  describe "query" do
    test "simple query" do
      {:ok, conn} = MyXQL.start_link(@opts)

      assert {:ok, %MyXQL.Result{columns: ["2*3", "4*5"], rows: [[6, 20]]}} =
               MyXQL.query(conn, "SELECT 2*3, 4*5")
    end

    test "invalid query" do
      {:ok, conn} = MyXQL.start_link(@opts)

      assert {:error, %MyXQL.Error{message: "Unknown column 'bad' in 'field list'"}} =
               MyXQL.query(conn, "SELECT bad")
    end

    test "query with multiple rows" do
      {:ok, conn} = MyXQL.start_link(@opts)

      MyXQL.query!(conn, "TRUNCATE TABLE integers")
      %MyXQL.Result{num_rows: 2} = MyXQL.query!(conn, "INSERT INTO integers VALUES (10), (20)")

      assert {:ok, %MyXQL.Result{columns: ["x"], rows: [[10], [20]]}} =
               MyXQL.query(conn, "SELECT * FROM integers")
    end
  end

  describe "prepared statements" do
    test "params" do
      {:ok, conn} = MyXQL.start_link(@opts)

      assert {:ok, %MyXQL.Result{rows: [[6]]}} = MyXQL.query(conn, "SELECT ? * ?", [2, 3])
    end

    test "prepare and then execute" do
      {:ok, conn} = MyXQL.start_link(@opts)

      {:ok, query} = MyXQL.prepare(conn, "", "SELECT ? * ?")
      assert {:ok, %MyXQL.Query{}, %MyXQL.Result{rows: [[6]]}} = MyXQL.execute(conn, query, [2, 3])
    end
  end
end
