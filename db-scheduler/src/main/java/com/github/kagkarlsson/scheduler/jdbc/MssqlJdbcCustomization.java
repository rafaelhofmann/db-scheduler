/**
 * Copyright (C) Gustav Karlsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler.jdbc;

import com.github.kagkarlsson.scheduler.task.Execution;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Calendar;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;

import static com.github.kagkarlsson.scheduler.StringUtils.truncate;

public class MssqlJdbcCustomization implements JdbcCustomization {

    @Override
    public String getName() {
        return "MSSQL";
    }

    @Override
    public void setInstant(PreparedStatement p, int index, Instant value) throws SQLException {
        p.setTimestamp(index, value != null ? Timestamp.from(value) : null, Calendar.getInstance(TimeZone.getTimeZone("UTC")));
    }

    @Override
    public Instant getInstant(ResultSet rs, String columnName) throws SQLException {
        return Optional.ofNullable(rs.getTimestamp(columnName)).map(Timestamp::toInstant).orElse(null);
    }

    @Override
    public boolean supportsExplicitQueryLimitPart() {
        return false;
    }

    @Override
    public String getQueryLimitPart(int limit) {
        return "";
    }

    @Override
    public boolean supportsLockAndFetch() {
        return true;
    }

    @Override
    public List<Execution> lockAndFetch(JdbcTaskRepositoryContext ctx, Instant now, int limit) {
        final JdbcTaskRepository.UnresolvedFilter unresolvedFilter = new JdbcTaskRepository.UnresolvedFilter(ctx.taskResolver.getUnresolved());

        String selectForUpdateQuery =
            " UPDATE " + ctx.tableName +
                " SET " + ctx.tableName + ".picked = ?, " +
                "     " + ctx.tableName + ".picked_by = ?, " +
                "     " + ctx.tableName + ".last_heartbeat = ?, " +
                "     " + ctx.tableName + ".version = " + ctx.tableName + ".version + 1 " +
                " OUTPUT [inserted].* " +
                " FROM ( " +
                "   SELECT TOP(?) ist2.task_name, ist2.task_instance " +
                "   FROM " + ctx.tableName + " ist2 WITH (ROWLOCK, READPAST, INDEX(fetch_lock_idx)) " +
                "   WHERE ist2.picked = ? AND ist2.execution_time <= ? " + unresolvedFilter.andCondition() +
                "   ORDER BY ist2.execution_time ASC " +
                " ) AS st2 " +
                " WHERE st2.task_name = " + ctx.tableName + ".task_name " +
                "   AND st2.task_instance = " + ctx.tableName + ".task_instance";

        return ctx.jdbcRunner.query(selectForUpdateQuery,
            ps -> {
                int index = 1;
                // Update
                ps.setBoolean(index++, true); // picked (new)
                ps.setString(index++, truncate(ctx.schedulerName.getName(), 50)); // picked_by
                setInstant(ps, index++, now); // last_heartbeat
                // Inner select
                ps.setInt(index++, limit); // limit
                ps.setBoolean(index++, false); // picked (old)
                setInstant(ps, index++, now); // execution_time
                unresolvedFilter.setParameters(ps, index);
            },
            ctx.resultSetMapper.get());
    }
}
