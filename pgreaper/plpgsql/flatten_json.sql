CREATE OR REPLACE FUNCTION get_col_names(table_name TEXT)
RETURNS setof text AS $$
BEGIN
    RETURN QUERY EXECUTE format('SELECT DISTINCT jsonb_object_keys(json_data) FROM %s',
            table_name);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION _get_col_type(table_name TEXT, col_name TEXT)
RETURNS
    setof text AS $$
BEGIN
    RETURN QUERY EXECUTE format('WITH type_count AS (SELECT jsonb_typeof(json_data->''%s'') FROM %s LIMIT 1000) SELECT DISTINCT jsonb_typeof FROM type_count',
        col_name, table_name);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_col_type(table_name TEXT, col_name TEXT) RETURNS
    text AS
$$
DECLARE
    type_ text;
    has_number bool = false;
    has_array bool = false;
    has_text bool = false;
    has_json bool = false;
    has_bool bool = false;
BEGIN
    FOR type_ IN (SELECT * FROM _get_col_type(table_name, col_name))
    LOOP
        IF type_ LIKE 'number'::text THEN
            has_number = true;
        ELSIF type_ LIKE 'string'::text THEN
            has_text = true;
        ELSIF type_ LIKE 'array'::text THEN
            has_array = true;
        ELSIF type_ LIKE 'boolean'::text THEN
            has_bool = true;
        ELSIF type_ LIKE 'object'::text THEN
            has_json = true;
        END IF;
    END LOOP;
    
    IF has_json OR has_array THEN
        return 'jsonb';
    ELSIF has_text THEN
        return 'text';
    ELSIF has_bool THEN
        return 'boolean';
    ELSIF has_number THEN
        return 'numeric';
    ELSE
        -- Default option
        return 'jsonb';
    END IF;    
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
            '(json_data->>' || '''' || name || '''' || ')' || '::' ||
            get_col_type(table_name, name) ||
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