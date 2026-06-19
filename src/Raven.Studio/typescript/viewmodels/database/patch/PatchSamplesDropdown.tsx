import React from "react";
import Dropdown from "react-bootstrap/Dropdown";
import { CustomDropdownToggle } from "components/common/Dropdown";
import SampleQueriesTabs from "components/common/sampleQueries/SampleQueriesTabs";
import { scripts, methodGroups } from "./patchSamplesData";

export interface PatchSamplesDropdownProps {
    onLoadScript: (script: string) => void;
}

export default function PatchSamplesDropdown({ onLoadScript }: PatchSamplesDropdownProps) {
    return (
        <Dropdown className="patch-samples-dropdown">
            <Dropdown.Toggle as={CustomDropdownToggle}>Samples</Dropdown.Toggle>
            <Dropdown.Menu className="patch-samples-dropdown-menu p-0">
                <SampleQueriesTabs scripts={scripts} methodGroups={methodGroups} onSelect={onLoadScript} />
            </Dropdown.Menu>
        </Dropdown>
    );
}
