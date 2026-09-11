import React, { useEffect, useRef, useState } from "react";
import moment from "moment";
import { useAsyncCallback } from "react-async-hook";
import Modal from "components/common/Modal";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import Spinner from "react-bootstrap/Spinner";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import RichAlert from "components/common/RichAlert";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import TimeSeriesRangePicker, { TimeSeriesRangeState } from "./TimeSeriesRangePicker";
import { FilterTimezone, wallOf, zoneLabel, FULL_FORMAT } from "./timeSeriesRange.utils";

const COUNT_DEBOUNCE_MS = 300;

export interface TimeSeriesRangeCount {
    count: number;
    // Whether count is the true total, or only a lower bound (the range has more than we fetched).
    exact: boolean;
}

type TimeSeriesRange = filterTimeSeriesDates<moment.Moment | null>;

interface DeleteTimeSeriesRangeModalProps {
    timeSeriesName: string;
    startDate: moment.Moment | null;
    endDate: moment.Moment | null;
    timezone?: FilterTimezone;
    resolveCount: (range: TimeSeriesRange) => Promise<TimeSeriesRangeCount>;
    onDelete: (range: TimeSeriesRange) => Promise<void>;
    close: () => void;
}

function rangeSignature(range: TimeSeriesRange): string {
    const stamp = (d: moment.Moment | null) => (d ? String(d.valueOf()) : "∅");
    return `${stamp(range.startDate)}|${stamp(range.endDate)}`;
}

function describeRange(range: TimeSeriesRange, timezone: FilterTimezone): string {
    const fmt = (d: moment.Moment) => wallOf(d, timezone).format(FULL_FORMAT);
    const zone = zoneLabel(timezone);
    const { startDate, endDate } = range;

    if (startDate && endDate) {
        return `entries from ${fmt(startDate)} to ${fmt(endDate)} (${zone}), inclusive of both bounds`;
    }
    if (endDate) {
        return `all entries up to and including ${fmt(endDate)} (${zone})`;
    }
    if (startDate) {
        return `all entries from ${fmt(startDate)} (${zone}) onward`;
    }
    return "all entries";
}

interface CountState {
    loading: boolean;
    result: TimeSeriesRangeCount | null;
    // Set when the count request failed for a reason other than an empty range (e.g. 500/network).
    error?: boolean;
}

export default function DeleteTimeSeriesRangeModal(props: DeleteTimeSeriesRangeModalProps) {
    const { timeSeriesName, startDate, endDate, timezone, resolveCount, onDelete, close } = props;

    const [range, setRange] = useState<TimeSeriesRangeState>(() => ({
        range: { startDate, endDate },
        canApply: true,
        timezone: timezone ?? "local",
    }));
    const [countState, setCountState] = useState<CountState>({ loading: true, result: null });

    const rangeRef = useRef(range.range);
    rangeRef.current = range.range;
    // Monotonic request id so a slow in-flight count can't overwrite a newer one.
    const reqIdRef = useRef(0);

    const rangeSig = rangeSignature(range.range);

    useEffect(() => {
        if (!range.canApply) {
            setCountState({ loading: false, result: null });
            return;
        }
        setCountState((prev) => ({ loading: true, result: prev.result }));
        const reqId = ++reqIdRef.current;
        const handle = setTimeout(() => {
            resolveCount(rangeRef.current)
                .then((res) => {
                    if (reqIdRef.current === reqId) {
                        setCountState({ loading: false, result: res });
                    }
                })
                .catch(() => {
                    if (reqIdRef.current === reqId) {
                        setCountState({ loading: false, result: null, error: true });
                    }
                });
        }, COUNT_DEBOUNCE_MS);
        return () => clearTimeout(handle);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [rangeSig, range.canApply]);

    const { loading, result, error } = countState;
    const known = !loading && result != null;
    const countFailed = !loading && !!error;
    const count = result?.count ?? 0;
    const isEmptyRange = known && count === 0;
    const showExactCount = known && result.exact && count > 0;
    const entryWord = count === 1 ? "entry" : "entries";

    const deleteLabel = showExactCount ? `Delete ${count.toLocaleString()} ${entryWord}` : "Delete range";
    const deleteDisabled = !range.canApply || loading || isEmptyRange;

    const asyncDelete = useAsyncCallback(async () => {
        await onDelete(range.range);
        close();
    });

    const handleDelete = () => {
        if (deleteDisabled || asyncDelete.loading) {
            return;
        }
        // Failure is surfaced by the delete command itself; keep the dialog open to retry.
        asyncDelete.execute().catch(() => {});
    };

    return (
        <Modal show size="lg" onHide={close} contentClassName="modal-border bulge-danger">
            <Modal.Header onCloseClick={close} className="vstack gap-1 align-items-start">
                <div className="hstack gap-2 align-items-center">
                    <Icon icon="trash" color="danger" margin="m-0" className="fs-3" />
                    <h3 className="mb-0">Delete range</h3>
                </div>
                <div className="text-muted">
                    Permanently delete every data point inside the specified range from{" "}
                    <strong>{timeSeriesName}</strong>. This can&apos;t be undone.
                </div>
            </Modal.Header>
            <Modal.Body>
                <TimeSeriesRangePicker
                    startDate={startDate}
                    endDate={endDate}
                    initialTimezone={timezone}
                    onChange={setRange}
                />

                {range.canApply && loading && (
                    <div className="text-muted mt-3 hstack gap-2 align-items-center">
                        <Spinner size="sm" />
                        <span>Checking how many entries match this range…</span>
                    </div>
                )}

                {range.canApply && countFailed && (
                    <RichAlert variant="warning" className="mt-3">
                        Couldn&apos;t determine how many entries match this range. You can still delete, but the count
                        is unknown.
                    </RichAlert>
                )}

                {range.canApply && !loading && isEmptyRange && (
                    <RichAlert variant="info" className="mt-3">
                        No entries match this range, so there&apos;s nothing to delete.
                    </RichAlert>
                )}

                {range.canApply && !loading && !error && !isEmptyRange && (
                    <RichAlert variant="danger" className="mt-3">
                        <div>
                            {showExactCount ? (
                                <>
                                    <strong>
                                        {count.toLocaleString()} {entryWord}
                                    </strong>{" "}
                                    will be deleted.
                                </>
                            ) : (
                                "This range contains more entries than can be counted here."
                            )}
                        </div>
                        <div>
                            You&apos;re deleting {describeRange(range.range, range.timezone)}.{" "}
                            {!showExactCount && "Narrow the range to see an exact count. "}
                            This can&apos;t be undone.
                        </div>
                    </RichAlert>
                )}
            </Modal.Body>
            <Modal.Footer>
                <Button variant="link" className="link-muted" onClick={close} disabled={asyncDelete.loading}>
                    Cancel
                </Button>
                <ConditionalPopover
                    conditions={[
                        { isActive: !range.canApply, message: "Complete the range to delete." },
                        { isActive: range.canApply && isEmptyRange, message: "No entries match this range." },
                    ]}
                >
                    <ButtonWithSpinner
                        variant="danger"
                        className="rounded-pill"
                        icon="trash"
                        isSpinning={asyncDelete.loading}
                        disabled={deleteDisabled}
                        onClick={handleDelete}
                    >
                        {asyncDelete.loading ? "Deleting…" : deleteLabel}
                    </ButtonWithSpinner>
                </ConditionalPopover>
            </Modal.Footer>
        </Modal>
    );
}
