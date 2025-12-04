-- Real-world test for ranges extension
-- Run with: duckdb -unsigned test.sql

-- Install and load the extension
SET autoinstall_known_extensions=1;
SET autoload_known_extensions=1;
INSTALL 'build/release/extension/ranges/ranges.duckdb_extension';
LOAD ranges;

-- Test basic construction
SELECT '=== Basic Construction ===' AS section;
SELECT int4range(1, 5) AS default_bounds;
SELECT int4range(1, 5, '[)') AS half_open;
SELECT int4range(1, 5, '[]') AS closed;
SELECT int4range(1, 5, '()') AS open;
SELECT int4range(1, 5, '(]') AS half_closed;

-- Test accessor functions
SELECT '=== Accessor Functions ===' AS section;
SELECT 
    int4range(10, 20, '[]') AS range,
    lower(int4range(10, 20, '[]')) AS lower_bound,
    upper(int4range(10, 20, '[]')) AS upper_bound,
    lower_inc(int4range(10, 20, '[]')) AS is_lower_inc,
    upper_inc(int4range(10, 20, '[]')) AS is_upper_inc,
    isempty(int4range(10, 20, '[]')) AS is_empty;

-- Test empty ranges
SELECT '=== Empty Ranges ===' AS section;
SELECT int4range(5, 1, '[)') AS reversed_empty;
SELECT int4range(1, 1, '()') AS point_open;
SELECT int4range(1, 1, '[]') AS point_closed;
SELECT isempty(int4range(5, 1, '[)')) AS is_empty_reversed;
SELECT isempty(int4range(1, 1, '()')) AS is_empty_point_open;
SELECT isempty(int4range(1, 1, '[]')) AS is_empty_point_closed;

-- Test overlaps
SELECT '=== Overlap Tests ===' AS section;
SELECT range_overlaps(int4range(1, 5), int4range(3, 7)) AS overlapping;
SELECT range_overlaps(int4range(1, 5), int4range(5, 10)) AS touching_excluded;
SELECT range_overlaps(int4range(1, 5, '[]'), int4range(5, 10, '[]')) AS touching_included;
SELECT range_overlaps(int4range(1, 5), int4range(6, 10)) AS non_overlapping;

-- Test contains
SELECT '=== Contains Tests ===' AS section;
SELECT range_contains(int4range(1, 10), 5) AS contains_5;
SELECT range_contains(int4range(1, 10), 1) AS contains_lower;
SELECT range_contains(int4range(1, 10), 10) AS contains_upper_exclusive;
SELECT range_contains(int4range(1, 10, '[]'), 10) AS contains_upper_inclusive;

-- Real-world example: Age categories
SELECT '=== Real World: Age Categories ===' AS section;
CREATE TABLE age_categories AS 
SELECT * FROM (VALUES
    ('Infant', int4range(0, 2, '[)')),
    ('Toddler', int4range(2, 4, '[)')),
    ('Child', int4range(4, 13, '[)')),
    ('Teen', int4range(13, 20, '[)')),
    ('Adult', int4range(20, 65, '[)')),
    ('Senior', int4range(65, 150, '[]'))
) AS t(category, age_range);

SELECT category, age_range::VARCHAR, lower(age_range), upper(age_range) 
FROM age_categories 
ORDER BY lower(age_range);

-- Find category for specific ages
SELECT 'Age 5 is:' AS query, category 
FROM age_categories 
WHERE range_contains(age_range, 5);

SELECT 'Age 25 is:' AS query, category 
FROM age_categories 
WHERE range_contains(age_range, 25);

SELECT 'Age 70 is:' AS query, category 
FROM age_categories 
WHERE range_contains(age_range, 70);

-- Real-world example: Time slots
SELECT '=== Real World: Time Slots ===' AS section;
CREATE TABLE meetings AS
SELECT * FROM (VALUES
    ('Standup', int4range(9, 10, '[)')),
    ('Design Review', int4range(10, 12, '[)')),
    ('Lunch', int4range(12, 13, '[]')),
    ('Sprint Planning', int4range(14, 16, '[)')),
    ('1-on-1', int4range(11, 12, '[)'))
) AS t(meeting, time_slot);

-- Find overlapping meetings
SELECT 
    m1.meeting AS meeting1,
    m2.meeting AS meeting2
FROM meetings m1, meetings m2
WHERE m1.meeting < m2.meeting
  AND range_overlaps(m1.time_slot, m2.time_slot);

-- Cleanup
DROP TABLE age_categories;
DROP TABLE meetings;

SELECT '=== All Tests Passed! ===' AS result;
