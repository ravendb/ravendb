import React from "react";
import { Spinner } from "reactstrap";

export function LoadingView() {
    return (
        <h3>
            <Spinner size="sm" className="me-2" /> Loading...
        </h3>
    );
}
