CREATE OR REPLACE FUNCTION get_col_names(table_name TEXT) RETURNS setof text AS $$
BEGIN
    RETURN QUERY EXECUTE format('SELECT DISTINCT jsonb_object_keys(json_data) FROM %s',
            table_name);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION flatten_json_query_builder(table_name TEXT) RETURNS
    text AS
$$
DECLARE
	name text;
    temp text[];
    stmt text = 'SELECT ';
BEGIN
    FOR name in (SELECT * FROM get_col_names(table_name))
    LOOP
        temp := array_append(temp,
            'json_data->' || '''' || name || '''' ||
            ' AS ' || sanitize_name(name));
    END LOOP;
    
    stmt := stmt || array_to_string(temp, ', ') || ' FROM ' || table_name;
    
    RETURN stmt;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION flatten_json(table_name TEXT) RETURNS VOID AS
$$
BEGIN
    EXECUTE format('CREATE TABLE %s_flat AS (%s)',
        table_name, flatten_json_query_builder(table_name));
    EXECUTE format('DROP TABLE %s', table_name);
    EXECUTE format('ALTER TABLE %s_flat RENAME TO %s',
        table_name, table_name);
END;
$$ LANGUAGE plpgsql;