import React from "react";
import "./UsedIdsPill.scss";

interface PotentialUnusedIdsPillProps {
    vector: string;
    onClick: () => void;
    isAdded: boolean;
}
export default function PotentialUnusedIdsPill(props: PotentialUnusedIdsPillProps) {
    const { vector, onClick, isAdded } = props;

    return (
        <div className={`potential-unused-id-pill ${isAdded ? "added" : ""}`} onClick={onClick} title={vector}>
            <strong>{vector}</strong>
        </div>
    );
}
