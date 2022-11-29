import { withBootstrap5, withStorybookContexts } from "../../test/storybookTestUtils";
import { Checkbox } from "./Checkbox";
import { ComponentMeta } from "@storybook/react";
import { DropdownPanel, UncontrolledButtonWithDropdownPanel } from "./DropdownPanel";
import React, { useCallback, useState } from "react";
import { Button } from "reactstrap";

export default {
    title: "Bits/Dropdown Panel",
    decorators: [withStorybookContexts, withBootstrap5],
    component: UncontrolledDropdown,
} as ComponentMeta<typeof UncontrolledDropdown>;

export function UncontrolledDropdown() {
    const [log, setLog] = useState<string[]>([]);

    const enabledClick = useCallback(() => {
        setLog((prev) => [...prev, "Button clicked"]);
    }, []);

    return (
        <div>
            <h3>This is uncontrolled dropdown panel</h3>

            <UncontrolledButtonWithDropdownPanel buttonText="I'm dropdown">
                <div>
                    <Button onClick={enabledClick}>Enabled button</Button>
                    <Button onClick={enabledClick} disabled>
                        Disabled button
                    </Button>
                    <Button className={DropdownPanel.closerClass}>I can close this dropdown</Button>
                </div>
            </UncontrolledButtonWithDropdownPanel>

            <pre className="mt-5">{JSON.stringify(log, null, 4)}</pre>
        </div>
    );
}
