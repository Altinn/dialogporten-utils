\set ON_ERROR_STOP on

\if :{?repair_name}
\else
\set repair_name has_unopened_content_repair_2026_02_25_2026_03_20
\endif

\if :{?batch_size}
\else
\set batch_size NULL
\endif

CALL maintenance.has_unopened_content_repair_discover(:'repair_name', :batch_size);

SELECT *
FROM maintenance.has_unopened_content_repair_summary
WHERE repair_name = :'repair_name';
