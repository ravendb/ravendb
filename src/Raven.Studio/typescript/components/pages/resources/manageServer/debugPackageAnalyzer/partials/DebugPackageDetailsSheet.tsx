import React from "react";
import { Icon } from "components/common/Icon";
import Code from "components/common/Code";
import { ViewSheet } from "components/common/splitView/ViewSheet";
import IconName from "typings/server/icons";
import { ThemeColor } from "components/models/common";

interface DebugPackageDetailsSheetProps {
    title: string;
    content: string;
    icon?: IconName;
    iconColor?: ThemeColor;
}

// Side panel that shows a full exception / detected-issue description, mirroring the indexing error
// details sheet. Used for both the analyzer's per-component failures and the detected-issue cards,
// whose text is too long to read comfortably inline.
export default function DebugPackageDetailsSheet({
    title,
    content,
    icon = "warning",
    iconColor = "warning",
}: DebugPackageDetailsSheetProps) {
    return (
        <ViewSheet>
            <ViewSheet.Header>
                <h3 className="mb-0 text-truncate" title={title}>
                    <Icon icon={icon} color={iconColor} />
                    {title}
                </h3>
            </ViewSheet.Header>
            <ViewSheet.Body className="m-2">
                <Code code={content} language="csharp" wrappable />
            </ViewSheet.Body>
        </ViewSheet>
    );
}
