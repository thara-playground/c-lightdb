require 'test/unit'

def run_script(commands, dbfile=nil)
  filename = dbfile || "mydb.db"

  raw_output = nil
  IO.popen("./cmake-build-debug/lightdb " + filename, "r+") do |pipe|
    commands.each do |command|
      pipe.puts command
    end

    pipe.close_write

    # Read entire output
    raw_output = pipe.gets(nil)
  end

  system("rm " + filename) if dbfile == nil

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

  def test_keeps_data_after_closing_connection
    dbfile = "keeps_data.db"

    result1 = run_script([
      "insert 1 user1 person1@example.com",
      ".exit",
    ], dbfile)
    assert_equal result1, [
      "db > Executed.",
      "db > ",
    ]
    result2 = run_script([
      "select",
      ".exit",
    ], dbfile)
    assert_equal result2, [
      "db > (1, user1, person1@example.com)",
      "Executed.",
      "db > ",
    ]

    system("rm " + dbfile)
  end

  def test_prints_constants
    result = run_script([
      ".constants",
      ".exit",
    ])
    assert_equal result, [
      "db > Constants:",
      "ROW_SIZE: 293",
      "COMMON_NODE_HEADER_SIZE: 6",
      "LEAF_NODE_HEADER_SIZE: 10",
      "LEAF_NODE_CELL_SIZE: 297",
      "LEAF_NODE_SPACE_FOR_CELLS: 4086",
      "LEAF_NODE_MAX_CELLS: 13",
      "db > ",
    ]
  end

  def test_allow_printing_out_the_structure_of_a_one_node_btree
    script = [3, 1, 2].map do |i|
      "insert #{i} user#{i} person#{i}@example.com"
    end
    script << ".btree"
    script << ".exit"
    result = run_script(script)

    assert_equal result, [
      "db > Executed.",
      "db > Executed.",
      "db > Executed.",
      "db > Tree:",
      "leaf (size 3)",
      "  - 0 : 3",
      "  - 1 : 1",
      "  - 2 : 2",
      "db > "
    ]
  end

end
