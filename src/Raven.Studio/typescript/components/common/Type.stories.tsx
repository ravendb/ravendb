import React from "react";
import { Card, CardBody } from "reactstrap";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";

export default {
    title: "Bits/Type",
    decorators: [withStorybookContexts, withBootstrap5],
};

export function Type() {
    return (
        <Card>
            <CardBody>
                <h1>Header 1</h1>
                <h2>Header 2</h2>
                <h3>Header 3</h3>
                <h4>Header 4</h4>
                <h5>Header 5</h5>
                <h6>Header 6</h6>

                <p>Regular text</p>
                <p className="text-muted">Text muted</p>
                <p className="text-emphasis">Text emphasis</p>
                <p className="lead">Text lead</p>
            </CardBody>
        </Card>
    );
}
