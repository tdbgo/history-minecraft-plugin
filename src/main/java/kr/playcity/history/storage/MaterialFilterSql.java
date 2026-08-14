package kr.playcity.history.storage;

import kr.playcity.history.model.HistoryQuery;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Set;

final class MaterialFilterSql {
    private MaterialFilterSql() {
    }

    static void append(StringBuilder sql, HistoryQuery query) {
        appendIncluded(sql, query.includedMaterials());
        for (int ignored = 0; ignored < query.excludedMaterials().size(); ignored++) {
            sql.append(" AND NOT ");
            appendOne(sql);
        }
    }

    static int bind(PreparedStatement statement, int parameter, HistoryQuery query) throws SQLException {
        parameter = bindKeys(statement, parameter, query.includedMaterials());
        return bindKeys(statement, parameter, query.excludedMaterials());
    }

    private static void appendIncluded(StringBuilder sql, Set<String> keys) {
        if (keys.isEmpty()) {
            return;
        }
        sql.append(" AND (");
        int index = 0;
        for (String ignored : keys) {
            if (index++ > 0) {
                sql.append(" OR ");
            }
            appendOne(sql);
        }
        sql.append(')');
    }

    private static void appendOne(StringBuilder sql) {
        sql.append("((bs1.block_data = ? OR bs1.block_data LIKE ? ESCAPE '\\')")
            .append(" OR (bs2.block_data = ? OR bs2.block_data LIKE ? ESCAPE '\\'))");
    }

    private static int bindKeys(PreparedStatement statement, int parameter, Set<String> keys) throws SQLException {
        for (String key : keys) {
            String statePattern = escapeLike(key) + "[%";
            statement.setString(parameter++, key);
            statement.setString(parameter++, statePattern);
            statement.setString(parameter++, key);
            statement.setString(parameter++, statePattern);
        }
        return parameter;
    }

    private static String escapeLike(String value) {
        return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
    }
}
