import React from "react";
import RichAlert from "components/common/RichAlert";
import { Button } from "reactstrap";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";

export default {
    title: "Bits/Rich Alert",
    component: RichAlert,
    decorators: [withStorybookContexts, withBootstrap5],
};

const colors = [
    "primary",
    "secondary",
    "success",
    "warning",
    "danger",
    "info",
    "progress",
    "node",
    "shard",
    "dark",
    "light",
];

export const Variants = () => {
    return (
        <div>
            <div className="vstack gap-1">
                <RichAlert variant="primary" title="Default alert">
                    This is an example of a primary alert that has a title.
                </RichAlert>
                <RichAlert variant="primary">This is an example of a primary alert without a title.</RichAlert>
                <RichAlert variant="success" icon="settings">
                    This is an example of a success alert with custom icon.
                </RichAlert>
            </div>
            <hr />
            <RichAlert variant="danger" icon="storage" iconAddon="settings" title="Danger alert">
                This is an example of a danger alert that has <strong>icon</strong> with <em>addon</em>. And this is{" "}
                <a href="#">some link</a>.
                <br />
                <div className="vstack gap-1">
                    Also why shouldn&apos;t we put here some button?
                    <Button variant="danger" size="sm" className="w-fit-content">
                        Free kittens here
                    </Button>
                </div>
            </RichAlert>
            <hr />
            <AllRichAlertColors />
        </div>
    );
};

function AllRichAlertColors() {
    return (
        <div className="vstack gap-1">
            {colors.map((variant) => (
                <RichAlert key={variant} variant={variant}>
                    This is an example of a {variant} alert without a title.
                </RichAlert>
            ))}
        </div>
    );
}
