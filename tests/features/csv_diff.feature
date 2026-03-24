Feature: CSV stream diff
  The tool compares large CSVs by key and writes machine-usable output files.

  Scenario: mapped columns show left-only, right-only, and changed values
    Given a comparison workspace
    And the left CSV contains
      | customer_id | transaction_date | amount | status | description |
      | C1          | 2026-01-01       | 10.00  | OPEN   | alpha       |
      | C2          | 2026-01-02       | 20.00  | OPEN   | beta        |
      | C3          | 2026-01-03       | 30.00  | CLOSED | gamma       |
    And the right CSV contains
      | cust_id | txn_dt     | transaction_amount | txn_status | desc         |
      | C1      | 2026-01-01 | 10.00              | OPEN       | alpha        |
      | C2      | 2026-01-02 | 25.00              | OPEN       | beta changed |
      | C4      | 2026-01-04 | 40.00              | OPEN       | delta        |
    And the default comparison config
    When I run the comparison
    Then the summary count "only_in_left" is 1
    And the summary count "only_in_right" is 1
    And the summary count "different_rows" is 1
    And the summary count "different_cells" is 2
    And the output file "differences" contains
      | key_customer_id | key_transaction_date | difference_count | differences_text                                                                     |
      | C2              | 2026-01-02           | 2                | amount/transaction_amount: 20.00 -> 25.00; description/desc: beta -> beta changed |

  Scenario: sampling compares an exact number of keys
    Given a comparison workspace
    And the left CSV contains
      | customer_id | transaction_date | amount | status | description |
      | C1          | 2026-01-01       | 10.00  | OPEN   | alpha       |
      | C2          | 2026-01-02       | 20.00  | OPEN   | beta        |
      | C3          | 2026-01-03       | 30.00  | OPEN   | gamma       |
      | C4          | 2026-01-04       | 40.00  | OPEN   | delta       |
    And the right CSV contains
      | cust_id | txn_dt     | transaction_amount | txn_status | desc  |
      | C1      | 2026-01-01 | 10.00              | OPEN       | alpha |
      | C2      | 2026-01-02 | 20.00              | OPEN       | beta  |
      | C3      | 2026-01-03 | 30.00              | OPEN       | gamma |
      | C4      | 2026-01-04 | 40.00              | OPEN       | delta |
    And sampling size is 2 with seed 99
    And the default comparison config
    When I run the comparison
    Then the summary count "matched" is 2
    And the sampling field "actual_size" is 2

  Scenario: duplicate keys are warned and the first occurrence is used
    Given a comparison workspace
    And the left CSV contains
      | customer_id | transaction_date | amount | status | description |
      | C1          | 2026-01-01       | 10.00  | OPEN   | first       |
      | C1          | 2026-01-01       | 99.00  | OPEN   | duplicate   |
    And the right CSV contains
      | cust_id | txn_dt     | transaction_amount | txn_status | desc  |
      | C1      | 2026-01-01 | 10.00              | OPEN       | first |
    And the default comparison config
    When I run the comparison
    Then the summary count "duplicate_keys_left" is 1
    And the summary count "different_rows" is 0
    And the summary warning contains "first occurrence"
