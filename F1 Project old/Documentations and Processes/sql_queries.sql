-- Basic queries for F1 database

-- View all constructors (teams)
SELECT * FROM constructors;

-- View all drivers with their teams
SELECT d.driver_id, d.driver_name, d.team_name, c.constructor_name 
FROM drivers d
JOIN constructors c ON d.team_name = c.constructor_name
ORDER BY c.constructor_name, d.driver_name;

-- View all tracks/circuits in the database
SELECT track_id, track_name, country_name, location 
FROM tracks
ORDER BY country_name, track_name;

-- Get a list of all races in the season
SELECT r.race_id, t.track_name, t.country_name, r.date
FROM race_results r
JOIN tracks t ON r.circuit_key = t.circuit_key
ORDER BY r.date;

-- Compare qualifying vs race positions for a specific circuit
SELECT 
  t.track_name,
  d.driver_name,
  c.constructor_name,
  q.position as quali_position,
  r.position as race_position,
  (q.position - r.position) as positions_gained
FROM race_results res
JOIN tracks t ON res.circuit_key = t.circuit_key
CROSS JOIN LATERAL jsonb_array_elements(res.quali_positions::jsonb) WITH ORDINALITY AS q(data, position)
CROSS JOIN LATERAL jsonb_array_elements(res.race_positions::jsonb) WITH ORDINALITY AS r(data, position)
JOIN drivers d ON (q.data->>'driver_id')::int = d.driver_id AND (r.data->>'driver_id')::int = d.driver_id
JOIN constructors c ON (q.data->>'constructor_id')::int = c.constructor_id
WHERE t.track_name = 'Monaco'
ORDER BY q.position;

-- Find drivers who improved the most positions from qualifying to race
SELECT 
  d.driver_name,
  c.constructor_name,
  t.track_name,
  q.position as quali_position,
  r.position as race_position,
  (q.position - r.position) as positions_gained
FROM race_results res
JOIN tracks t ON res.circuit_key = t.circuit_key
CROSS JOIN LATERAL jsonb_array_elements(res.quali_positions::jsonb) WITH ORDINALITY AS q(data, position)
CROSS JOIN LATERAL jsonb_array_elements(res.race_positions::jsonb) WITH ORDINALITY AS r(data, position)
JOIN drivers d ON (q.data->>'driver_id')::int = d.driver_id AND (r.data->>'driver_id')::int = d.driver_id
JOIN constructors c ON (q.data->>'constructor_id')::int = c.constructor_id
ORDER BY positions_gained DESC
LIMIT 10;

-- Count podium finishes for each driver (positions 1-3)
SELECT 
  d.driver_name,
  c.constructor_name,
  COUNT(*) as podium_count
FROM race_results res
CROSS JOIN LATERAL jsonb_array_elements(res.race_positions::jsonb) WITH ORDINALITY AS r(data, position)
JOIN drivers d ON (r.data->>'driver_id')::int = d.driver_id
JOIN constructors c ON (r.data->>'constructor_id')::int = c.constructor_id
WHERE r.position <= 3
GROUP BY d.driver_name, c.constructor_name
ORDER BY podium_count DESC;

-- Calculate points for each driver (simplified F1 points system)
WITH points_system AS (
    SELECT 1 as position, 25 as points UNION
    SELECT 2, 18 UNION
    SELECT 3, 15 UNION
    SELECT 4, 12 UNION
    SELECT 5, 10 UNION
    SELECT 6, 8 UNION
    SELECT 7, 6 UNION
    SELECT 8, 4 UNION
    SELECT 9, 2 UNION
    SELECT 10, 1
)
SELECT 
  d.driver_name,
  c.constructor_name,
  SUM(COALESCE(p.points, 0)) as total_points
FROM race_results res
CROSS JOIN LATERAL jsonb_array_elements(res.race_positions::jsonb) WITH ORDINALITY AS r(data, position)
JOIN drivers d ON (r.data->>'driver_id')::int = d.driver_id
JOIN constructors c ON (r.data->>'constructor_id')::int = c.constructor_id
LEFT JOIN points_system p ON r.position = p.position
GROUP BY d.driver_name, c.constructor_name
ORDER BY total_points DESC;

-- Calculate constructor championship points
WITH points_system AS (
    SELECT 1 as position, 25 as points UNION
    SELECT 2, 18 UNION
    SELECT 3, 15 UNION
    SELECT 4, 12 UNION
    SELECT 5, 10 UNION
    SELECT 6, 8 UNION
    SELECT 7, 6 UNION
    SELECT 8, 4 UNION
    SELECT 9, 2 UNION
    SELECT 10, 1
)
SELECT 
  c.constructor_name,
  SUM(COALESCE(p.points, 0)) as total_points
FROM race_results res
CROSS JOIN LATERAL jsonb_array_elements(res.race_positions::jsonb) WITH ORDINALITY AS r(data, position)
JOIN constructors c ON (r.data->>'constructor_id')::int = c.constructor_id
LEFT JOIN points_system p ON r.position = p.position
GROUP BY c.constructor_name
ORDER BY total_points DESC;

-- Find each driver's best finishing position in the season
SELECT
  d.driver_name,
  c.constructor_name,
  MIN(r.position) as best_finish,
  ARRAY_AGG(t.track_name ORDER BY r.position) as tracks
FROM race_results res
JOIN tracks t ON res.circuit_key = t.circuit_key
CROSS JOIN LATERAL jsonb_array_elements(res.race_positions::jsonb) WITH ORDINALITY AS r(data, position)
JOIN drivers d ON (r.data->>'driver_id')::int = d.driver_id
JOIN constructors c ON (r.data->>'constructor_id')::int = c.constructor_id
GROUP BY d.driver_name, c.constructor_name
ORDER BY best_finish, d.driver_name;

-- Compare teammate qualifying performance
SELECT 
  c.constructor_name as team,
  d1.driver_name as driver1,
  d2.driver_name as driver2,
  COUNT(CASE WHEN q1.position < q2.position THEN 1 END) as driver1_ahead,
  COUNT(CASE WHEN q2.position < q1.position THEN 1 END) as driver2_ahead
FROM race_results res
CROSS JOIN LATERAL jsonb_array_elements(res.quali_positions::jsonb) WITH ORDINALITY AS q1(data, position)
CROSS JOIN LATERAL jsonb_array_elements(res.quali_positions::jsonb) WITH ORDINALITY AS q2(data, position)
JOIN drivers d1 ON (q1.data->>'driver_id')::int = d1.driver_id
JOIN drivers d2 ON (q2.data->>'driver_id')::int = d2.driver_id
JOIN constructors c ON (q1.data->>'constructor_id')::int = c.constructor_id
WHERE 
  d1.team_name = d2.team_name AND
  d1.driver_id < d2.driver_id
GROUP BY c.constructor_name, d1.driver_name, d2.driver_name
ORDER BY c.constructor_name;
