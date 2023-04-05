import React from "react";
import { Spinner } from "reactstrap";

export function LoadingView() {
    return (
        <div className="d-flex justify-content-center align-items-center flex-column gap-3 mt-4">
            <Spinner className="spinner-gradient" />
            <h3>Loading</h3>
        </div>
    );
}
