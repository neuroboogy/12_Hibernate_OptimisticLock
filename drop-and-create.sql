DROP TABLE IF EXISTS items CASCADE;
CREATE TABLE items (id serial PRIMARY KEY, val int, version serial);
