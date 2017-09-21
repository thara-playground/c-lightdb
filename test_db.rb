require 'test/unit'

def run_script(commands)
  raw_output = nil
  IO.popen("./cmake-build-debug/lightdb", "r+") do |pipe|
    commands.each do |command|
      pipe.puts command
    end

    pipe.close_write

    # Read entire output
    raw_output = pipe.gets(nil)
  end
  raw_output.split("\n")
end

class TestDB < Test::Unit::TestCase

  def test_inserts_and_retreives_a_row
    result = run_script([
      "insert 1 user1 person1@example.com",
      "select",
      ".exit",
    ])
    assert_equal result, [
      "db > Executed.",
      "db > (1, user1, person1@example.com)",
      "Executed.",
      "db > ",
    ]
  end

  def test_prints_error_message_when_table_is_full
    script = (1..1401).map do |i|
      "insert #{i} user#{i} person#{i}@example.com"
    end
    script << ".exit"
    result = run_script(script)
    assert_equal result[-2], 'db > Error: Table full.'
  end

  def test_allows_inserting_strings_that_are_the_maximum_length
    long_username = "a"*32
    long_email = "a"*255
    script = [
      "insert 1 #{long_username} #{long_email}",
      "select",
      ".exit",
    ]
    result = run_script(script)
    assert_equal result, [
      "db > Executed.",
      "db > (1, #{long_username}, #{long_email})",
      "Executed.",
      "db > ",
    ]
  end

  def test_prints_error_message_if_strings_are_too_long
    long_username = "a"*33
    long_email = "a"*256
    script = [
      "insert 1 #{long_username} #{long_email}",
      "select",
      ".exit",
    ]
    result = run_script(script)
    assert_equal result, [
      "db > String is too long.",
      "db > Executed.",
      "db > ",
    ]
  end

def test_prints_an_error_message_if_id_is_negative
  script = [
    "insert -1 cstack foo@bar.com",
    "select",
    ".exit",
  ]
  result = run_script(script)
  assert_equal result, [
    "db > ID must be positive.",
    "db > Executed.",
    "db > ",
  ]
end

end
