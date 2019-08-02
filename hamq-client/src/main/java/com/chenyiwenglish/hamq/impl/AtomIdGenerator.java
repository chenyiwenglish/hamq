package com.chenyiwenglish.hamq.impl;

import com.chenyiwenglish.hamq.MessageIdGenerator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

@Slf4j
public class AtomIdGenerator implements MessageIdGenerator {

    private Map<String, ConcurrentLinkedQueue<Long>> idCacheMap = new HashMap<>();

    private static final String IDONLY_SQL = "select id from Idonly.genid where type_name = '%s' limit %d";

    private static final int BATCH_GENERATE_COUNT = 1000;

    private RowMapper<Long> mapper = new RowMapper<Long>() {

        @Override
        public Long mapRow(ResultSet rs, int rowNum) throws SQLException {
            return rs.getLong("id");
        }
    };

    private JdbcTemplate jdbcTemplate;

    private String atomType;

    public AtomIdGenerator(JdbcTemplate jdbcTemplate, String atomType) {
        this.jdbcTemplate = jdbcTemplate;
        this.atomType = atomType;
    }

    public Long generateId(Integer atomMaxCount) {
        if (atomMaxCount == null || atomMaxCount <= 0) {
            atomMaxCount = BATCH_GENERATE_COUNT;
        }
        if (!idCacheMap.containsKey(atomType)) {
            ConcurrentLinkedQueue<Long> idCache = new ConcurrentLinkedQueue<>();
            idCacheMap.put(atomType, idCache);
        }
        ConcurrentLinkedQueue<Long> idCache = idCacheMap.get(atomType);
        Long id = idCache.poll();
        if (id == null) {
            int retryCount = 0;
            while (true) {
                try {
                    if (atomMaxCount > 5000) {
                        atomMaxCount = 5000;
                    }
                    List<Long> ids = jdbcTemplate.query(String.format(IDONLY_SQL, atomType, atomMaxCount), mapper);
                    idCache.addAll(ids);
                    id = idCache.poll();
                } catch (Throwable e) {
                    // ignore
                }
                if (id != null) {
                    break;
                } else {
                    ++retryCount;
                    if (retryCount > 1) {
                        throw new RuntimeException("atom generate id failed");
                    }
                }
            }
        }
        return id;
    }

    public Long generateId() {
        return this.generateId(null);
    }

}

