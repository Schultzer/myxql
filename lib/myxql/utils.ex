defmodule MyXQL.Utils do
  @moduledoc false
  use Bitwise

  # https://dev.mysql.com/doc/internals/en/secure-password-authentication.html
  def mysql_native_password(password, auth_plugin_data)
      when is_binary(password) and is_binary(auth_plugin_data) do
    password_sha1 = :crypto.hash(:sha, password)

    bxor_binary(
      password_sha1,
      :crypto.hash(:sha, auth_plugin_data <> :crypto.hash(:sha, password_sha1))
    )
  end

  # https://dev.mysql.com/doc/internals/en/sha256.html
  def sha256_password(password, salt) do
    stage1 = :crypto.hash(:sha256, password)
    stage2 = :crypto.hash(:sha256, stage1)
    :crypto.hash_init(:sha256)
    |> :crypto.hash_update(stage2)
    |> :crypto.hash_update(salt)
    |> :crypto.hash_final
    |> bxor_binary(stage1)
  end

  defp bxor_binary(b1, b2) do
    for {e1, e2} <- List.zip([:erlang.binary_to_list(b1), :erlang.binary_to_list(b2)]) do
      e1 ^^^ e2
    end
    |> :erlang.list_to_binary()
  end
end
