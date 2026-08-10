import React, { useState } from "react";
import moment from "moment";
import Modal from "components/common/Modal";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import TimeSeriesRangePicker, { FilterTimezone, TimeSeriesRangeState } from "./TimeSeriesRangePicker";

interface FilterTimeSeriesModalProps {
    startDate: moment.Moment | null;
    endDate: moment.Moment | null;
    // Time zone currently selected in the time series grid. Seeds the picker so the
    // filter opens speaking the same zone the user sees in the table.
    timezone?: FilterTimezone;
    onApply: (dates: filterTimeSeriesDates<moment.Moment>, timezone: FilterTimezone) => void;
    close: () => void;
}

export default function FilterTimeSeriesModal({
    startDate,
    endDate,
    timezone,
    onApply,
    close,
}: FilterTimeSeriesModalProps) {
    const [state, setState] = useState<TimeSeriesRangeState>(() => ({
        range: { startDate, endDate },
        canApply: true,
        timezone: timezone ?? "local",
    }));

    const handleApply = () => {
        if (!state.canApply) {
            return;
        }
        onApply(state.range, state.timezone);
        close();
    };

    return (
        <Modal show size="lg" onHide={close} contentClassName="modal-border bulge-primary">
            <Modal.Header onCloseClick={close} className="vstack gap-1 align-items-start">
                <div className="hstack gap-2 align-items-center">
                    <Icon icon="calendar" color="primary" margin="m-0" className="fs-3" />
                    <h3 className="mb-0">Filter time series</h3>
                </div>
                <div className="text-muted">
                    Filter data points that occurred within the specified start and end timestamps.
                </div>
            </Modal.Header>
            <Modal.Body>
                <TimeSeriesRangePicker
                    startDate={startDate}
                    endDate={endDate}
                    initialTimezone={timezone}
                    onChange={setState}
                />
            </Modal.Body>
            <Modal.Footer>
                <Button variant="link" className="link-muted" onClick={close}>
                    Cancel
                </Button>
                <Button variant="primary" className="rounded-pill" disabled={!state.canApply} onClick={handleApply}>
                    Apply filter
                </Button>
            </Modal.Footer>
        </Modal>
    );
}
