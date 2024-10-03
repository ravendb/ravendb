import React from "react";
import "./ProgressBarWithTrackingPoint.scss";
import { UncontrolledTooltip } from "reactstrap";
import useId from "hooks/useId";

interface ProgressBarWithTrackingPointProps {
    startingPoint: number;
    progress: number;
    endingPoint: number;
}

export default function ProgressBarWithTrackingPoint(props: ProgressBarWithTrackingPointProps) {
    const { startingPoint, progress, endingPoint } = props;
    const totalRange = endingPoint - startingPoint;
    const progressPercentage = ((progress - startingPoint) / totalRange) * 100;

    const uniqueTrackingPointStartId = useId("trackingPointStart");
    const uniqueTrackingPointProgressId = useId("trackingPointProgress");
    const uniqueTrackingPointEndId = useId("trackingPointEnd");

    return (
        <div className="d-inline-block position-relative w-100">
            <div className="progress" style={{ height: "7.082485644px" }}>
                <div
                    className="progress-bar bg-progress"
                    role="progressbar"
                    style={{ width: `${progressPercentage}%` }}
                    aria-valuenow={progress}
                    aria-valuemin={startingPoint}
                    aria-valuemax={endingPoint}
                ></div>
            </div>
            <div className="tracking-point tracking-point--start" id={uniqueTrackingPointStartId} />
            <div
                className="tracking-point tracking-point--progress"
                id={uniqueTrackingPointProgressId}
                style={{ left: `${progressPercentage}%` }}
            />
            <div className="tracking-point tracking-point--end" id={uniqueTrackingPointEndId} />
            <UncontrolledTooltip placement="top" target={uniqueTrackingPointStartId}>
                {startingPoint}
            </UncontrolledTooltip>
            <UncontrolledTooltip placement="top" target={uniqueTrackingPointProgressId}>
                {progress}
            </UncontrolledTooltip>
            <UncontrolledTooltip placement="top" target={uniqueTrackingPointEndId}>
                {endingPoint}
            </UncontrolledTooltip>
        </div>
    );
}
