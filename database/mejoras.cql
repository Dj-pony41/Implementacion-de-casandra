-- Optimización del keyspace (recomendado para producción)
ALTER KEYSPACE semapa_v3 WITH replication = {
    'class': 'NetworkTopologyStrategy', 
    'replication_factor': 3  -- Aumentar si tienes más nodos
};

-- Optimización de la tabla lecturas_medidor (la más crítica)
ALTER TABLE semapa_v3.lecturas_medidor WITH
    compaction = {
        'class': 'TimeWindowCompactionStrategy',
        'compaction_window_unit': 'DAYS',
        'compaction_window_size': 1,
        'tombstone_threshold': 0.1
    }
    AND memtable_flush_period_in_ms = 3600000  -- 1 hora
    AND memtable_heap_space_in_mb = 2048        -- 2GB
    AND memtable_offheap_space_in_mb = 2048    -- 2GB
    AND default_time_to_live = 0
    AND gc_grace_seconds = 86400;              -- 1 día

-- Optimización de la tabla errores_iot
ALTER TABLE semapa_v3.errores_iot WITH
    compaction = {
        'class': 'LeveledCompactionStrategy',
        'sstable_size_in_mb': 160
    }
    AND memtable_flush_period_in_ms = 3600000;