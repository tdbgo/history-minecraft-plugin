package kr.playcity.history.storage;

import kr.playcity.history.model.BlockPosition;
import kr.playcity.history.model.ChangeCause;
import kr.playcity.history.model.ChangeRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Compacts duplicate and continuous transitions to rollback-equivalent endpoints.
 * Any non-batch change at the same coordinate is an explicit compaction barrier.
 */
final class WorldEditBatchCompactor {
    private WorldEditBatchCompactor() {
    }

    static List<ChangeRecord> compact(List<ChangeRecord> input) {
        Objects.requireNonNull(input, "input");
        if (input.size() < 2) {
            return List.copyOf(input);
        }

        List<ChangeRecord> output = new ArrayList<>(input.size());
        Map<BlockPosition, Candidate> candidates = new HashMap<>();
        for (ChangeRecord change : input) {
            Objects.requireNonNull(change, "change");
            if (!compressible(change)) {
                candidates.remove(change.position());
                output.add(change);
                continue;
            }

            Candidate candidate = candidates.get(change.position());
            if (candidate == null || !candidate.compatible(change)) {
                if (candidate != null) {
                    candidates.remove(change.position());
                }
                int outputIndex = output.size();
                output.add(change);
                candidates.put(change.position(), Candidate.start(change, outputIndex));
                continue;
            }

            if (candidate.duplicateOf(change)) {
                continue;
            }
            if (!candidate.currentAfter().sameState(change.before(), true)) {
                int outputIndex = output.size();
                output.add(change);
                candidates.put(change.position(), Candidate.start(change, outputIndex));
                continue;
            }

            ChangeRecord merged = candidate.merge(change);
            if (merged.before().sameState(merged.after(), true)) {
                if (candidate.outputIndex() >= 0) {
                    output.set(candidate.outputIndex(), null);
                }
                candidates.put(change.position(), candidate.withoutOutput(merged));
            } else if (candidate.outputIndex() >= 0) {
                output.set(candidate.outputIndex(), merged);
                candidates.put(change.position(), candidate.withRecord(merged));
            } else {
                int outputIndex = output.size();
                output.add(merged);
                candidates.put(change.position(), candidate.withRecord(merged, outputIndex));
            }
        }

        return output.stream().filter(Objects::nonNull).toList();
    }

    private static boolean compressible(ChangeRecord change) {
        return change.cause() == ChangeCause.WORLD_EDIT
            && change.batchId() != null
            && change.operationId() == null;
    }

    private record Candidate(ChangeRecord record, int outputIndex) {
        private static Candidate start(ChangeRecord record, int outputIndex) {
            return new Candidate(record, outputIndex);
        }

        private boolean compatible(ChangeRecord other) {
            UUID batchId = record.batchId();
            return batchId != null
                && batchId.equals(other.batchId())
                && record.actor().equals(other.actor())
                && record.cause() == other.cause()
                && record.metadata().equals(other.metadata());
        }

        private boolean duplicateOf(ChangeRecord other) {
            return record.before().sameState(other.before(), true)
                && record.after().sameState(other.after(), true);
        }

        private kr.playcity.history.model.BlockSnapshot currentAfter() {
            return record.after();
        }

        private ChangeRecord merge(ChangeRecord latest) {
            return new ChangeRecord(
                latest.id(),
                latest.occurredAt(),
                latest.position(),
                latest.actor(),
                latest.cause(),
                record.before(),
                latest.after(),
                null,
                latest.batchId(),
                latest.metadata(),
                latest.captureId()
            );
        }

        private Candidate withRecord(ChangeRecord replacement) {
            return new Candidate(replacement, outputIndex);
        }

        private Candidate withRecord(ChangeRecord replacement, int replacementIndex) {
            return new Candidate(replacement, replacementIndex);
        }

        private Candidate withoutOutput(ChangeRecord replacement) {
            return new Candidate(replacement, -1);
        }
    }
}
