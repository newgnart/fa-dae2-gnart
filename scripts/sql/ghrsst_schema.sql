-- Schema for storing full GHRSST data points based on actual NC4 file structure
-- Data: 1401 x 1001 grid points covering Vietnam region (lat 8-22N, lon 102-112E)
-- Variables: analysed_sst, analysis_error, sst_anomaly, mask, sea_ice_fraction, dt_1km_data

-- Create table for full GHRSST data points
CREATE TABLE IF NOT EXISTS staging.ghrsst_data (
    id BIGSERIAL PRIMARY KEY,
    file_name VARCHAR(255) NOT NULL,
    time_stamp TIMESTAMP NOT NULL,
    latitude DECIMAL(8,5) NOT NULL,     -- degrees_north, range 8.0 to 22.0
    longitude DECIMAL(8,5) NOT NULL,    -- degrees_east, range 102.0 to 112.0
    analysed_sst DECIMAL(8,3),          -- kelvin, range ~301.7 to 304.7 (28.6°C to 31.6°C)
    analysis_error DECIMAL(5,3),        -- kelvin, range ~0.37 to 0.42
    sst_anomaly DECIMAL(6,3),           -- kelvin, range ~-0.541 to 2.228
    mask SMALLINT,                      -- sea/land field composite mask, values 1-5
    sea_ice_fraction DECIMAL(5,4),      -- sea_ice_area_fraction (0-1), mostly NULL for Vietnam region
    dt_1km_data_seconds BIGINT,         -- converted from timedelta64 to seconds
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for efficient spatial-temporal queries
CREATE INDEX IF NOT EXISTS idx_ghrsst_data_time ON staging.ghrsst_data (time_stamp);
CREATE INDEX IF NOT EXISTS idx_ghrsst_data_location ON staging.ghrsst_data (latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_ghrsst_data_file ON staging.ghrsst_data (file_name);

-- Index on SST values (excluding NULLs for water areas only)
CREATE INDEX IF NOT EXISTS idx_ghrsst_data_sst ON staging.ghrsst_data (analysed_sst)
WHERE analysed_sst IS NOT NULL;

-- Index on mask for filtering by sea/land type
CREATE INDEX IF NOT EXISTS idx_ghrsst_data_mask ON staging.ghrsst_data (mask);

-- Composite index for common spatial-temporal queries
CREATE INDEX IF NOT EXISTS idx_ghrsst_data_time_location ON staging.ghrsst_data (time_stamp, latitude, longitude);

-- Spatial index for geographic queries (if PostGIS is available, this could be a geometry column)
CREATE INDEX IF NOT EXISTS idx_ghrsst_data_spatial ON staging.ghrsst_data (latitude, longitude)
WHERE analysed_sst IS NOT NULL;

-- Grant permissions
GRANT ALL PRIVILEGES ON staging.ghrsst_data TO postgres;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA staging TO postgres;

-- Add comments for documentation
COMMENT ON TABLE staging.ghrsst_data IS 'Full GHRSST L4 sea surface temperature data points from NASA MUR dataset';
COMMENT ON COLUMN staging.ghrsst_data.analysed_sst IS 'Sea surface foundation temperature in Kelvin (add -273.15 for Celsius)';
COMMENT ON COLUMN staging.ghrsst_data.analysis_error IS 'Estimated error standard deviation of analysed_sst in Kelvin';
COMMENT ON COLUMN staging.ghrsst_data.sst_anomaly IS 'SST anomaly from MUR 2003-2014 seasonal climatology in Kelvin';
COMMENT ON COLUMN staging.ghrsst_data.mask IS 'Sea/land composite mask: 1=open_sea, 2=land, 3=lake, 4=ice, 5=invalid';
COMMENT ON COLUMN staging.ghrsst_data.sea_ice_fraction IS 'Sea ice area fraction (0-1), typically NULL for tropical regions';
COMMENT ON COLUMN staging.ghrsst_data.dt_1km_data_seconds IS 'Time in seconds to most recent 1km resolution data';