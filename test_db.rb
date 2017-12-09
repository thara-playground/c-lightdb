require 'test/unit'

def run_script(commands, dbfile=nil)
  filename = dbfile || "mydb.db"

  raw_output = nil
  IO.popen("./cmake-build-debug/lightdb " + filename, "r+") do |pipe|
    commands.each do |command|
        begin
          pipe.puts command
        rescue Errno::EPIPE
          break
        end
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
    assert_equal result.last(2), [
      "db > Executed.",
      "db > Need to implement splitting internal node",
    ]
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
      "LEAF_NODE_HEADER_SIZE: 14",
      "LEAF_NODE_CELL_SIZE: 297",
      "LEAF_NODE_SPACE_FOR_CELLS: 4082",
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
      "- leaf (size 3)",
      "  - 1",
      "  - 2",
      "  - 3",
      "db > "
    ]
  end

  def test_prints_an_error_message_if_there_is_a_duplicate_id
    script = [
      "insert 1 user1 person1@example.com",
      "insert 1 user1 person1@example.com",
      "select",
      ".exit"
    ]
    result = run_script(script)
    assert_equal result, [
      "db > Executed.",
      "db > Error: Duplicate key.",
      "db > (1, user1, person1@example.com)",
      "Executed.",
      "db > ",
    ]
  end

  def test_allows_printing_out_the_structure_of_a_3_leaf_node_btree
    script = (1..14).map do |i|
      "insert #{i} user#{i} person#{i}@example.com"
    end
    script << ".btree"
    script << "insert 15 user15 person15@example.com"
    script << ".exit"
    result = run_script(script)
    assert_equal result[14..(result.length)], [
      "db > Tree:",
      "- internal (size 1)",
      "  - leaf (size 7)",
      "    - 1",
      "    - 2",
      "    - 3",
      "    - 4",
      "    - 5",
      "    - 6",
      "    - 7",
      "- key 7",
      "  - leaf (size 7)",
      "    - 8",
      "    - 9",
      "    - 10",
      "    - 11",
      "    - 12",
      "    - 13",
      "    - 14",
      "db > Executed.",
      "db > ",
    ]
  end

  def test_prints_all_rows_in_a_multi_level_tree
    script = (1..15).map do |i|
      "insert #{i} user#{i} person#{i}@example.com"
    end
    script << "select"
    script << ".exit"
    result = run_script(script)
    assert_equal result[15..(result.length)], [
      "db > (1, user1, person1@example.com)",
      "(2, user2, person2@example.com)",
      "(3, user3, person3@example.com)",
      "(4, user4, person4@example.com)",
      "(5, user5, person5@example.com)",
      "(6, user6, person6@example.com)",
      "(7, user7, person7@example.com)",
      "(8, user8, person8@example.com)",
      "(9, user9, person9@example.com)",
      "(10, user10, person10@example.com)",
      "(11, user11, person11@example.com)",
      "(12, user12, person12@example.com)",
      "(13, user13, person13@example.com)",
      "(14, user14, person14@example.com)",
      "(15, user15, person15@example.com)",
      "Executed.",
      "db > ",
    ]
  end

  def test_allows_printing_out_the_structure_of_a_4_leaf_node_btree
    script = [
      "insert 18 user18 person18@example.com",
      "insert 7 user7 person7@example.com",
      "insert 10 user10 person10@example.com",
      "insert 29 user29 person29@example.com",
      "insert 23 user23 person23@example.com",
      "insert 4 user4 person4@example.com",
      "insert 14 user14 person14@example.com",
      "insert 30 user30 person30@example.com",
      "insert 15 user15 person15@example.com",
      "insert 26 user26 person26@example.com",
      "insert 22 user22 person22@example.com",
      "insert 19 user19 person19@example.com",
      "insert 2 user2 person2@example.com",
      "insert 1 user1 person1@example.com",
      "insert 21 user21 person21@example.com",
      "insert 11 user11 person11@example.com",
      "insert 6 user6 person6@example.com",
      "insert 20 user20 person20@example.com",
      "insert 5 user5 person5@example.com",
      "insert 8 user8 person8@example.com",
      "insert 9 user9 person9@example.com",
      "insert 3 user3 person3@example.com",
      "insert 12 user12 person12@example.com",
      "insert 27 user27 person27@example.com",
      "insert 17 user17 person17@example.com",
      "insert 16 user16 person16@example.com",
      "insert 13 user13 person13@example.com",
      "insert 24 user24 person24@example.com",
      "insert 25 user25 person25@example.com",
      "insert 28 user28 person28@example.com",
      ".btree",
      ".exit",
    ]
    result = run_script(script)
    #assert_equal result, []
  end

end
