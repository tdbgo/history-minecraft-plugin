package kr.playcity.history.storage;

import kr.playcity.history.model.ChangeCause;
import kr.playcity.history.model.HistoryQuery;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

final class RollbackCauseFilterSql {
    private static final List<ChangeCause> ELIGIBLE = Arrays.stream(ChangeCause.values())
        .filter(ChangeCause::rollbackEligible)
        .toList();

    private RollbackCauseFilterSql() {
    }

    static void append(StringBuilder sql, HistoryQuery query) {
        if (!query.rollbackOnly()) {
            return;
        }
        appendEligible(sql, "c.cause");
    }

    static void appendEligible(StringBuilder sql, String column) {
        sql.append(" AND ").append(column).append(" IN (");
        for (int index = 0; index < ELIGIBLE.size(); index++) {
            if (index > 0) {
                sql.append(',');
            }
            sql.append('?');
        }
        sql.append(')');
    }

    static int bind(PreparedStatement statement, int parameter, HistoryQuery query) throws SQLException {
        if (!query.rollbackOnly()) {
            return parameter;
        }
        return bindEligible(statement, parameter);
    }

    static int bindEligible(PreparedStatement statement, int parameter) throws SQLException {
        for (ChangeCause cause : ELIGIBLE) {
            statement.setInt(parameter++, cause.storageCode());
        }
        return parameter;
    }
}
