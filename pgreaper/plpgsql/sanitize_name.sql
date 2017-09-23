CREATE OR REPLACE FUNCTION sanitize_name(col_name TEXT) RETURNS text AS $$
BEGIN
    RETURN regexp_replace(lower(col_name), '\s', '_');
END;
$$ LANGUAGE plpgsql;