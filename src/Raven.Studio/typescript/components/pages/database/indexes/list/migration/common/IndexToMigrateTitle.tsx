import { Icon } from "components/common/Icon";
import useId from "components/hooks/useId";
import { IndexSharedInfo } from "components/models/indexes";
import IndexUtils from "components/utils/IndexUtils";
import React from "react";
import { UncontrolledTooltip } from "reactstrap";
import { FlexGrow } from "components/common/FlexGrow";
import "./IndexesImportExport.scss";

interface IndexToMigrateTitleProps {
    index: Raven.Client.Documents.Indexes.IndexDefinition | IndexSharedInfo;
    disabledReason?: string;
}

export default function IndexToMigrateTitle({ index, disabledReason }: IndexToMigrateTitleProps) {
    const name = "Name" in index ? index.Name : index.name;
    const type = "Type" in index ? index.Type : index.type;

    const indexDisabledReasonTooltipId = useId("indexDisabledReasonId-");
    const indexLanguageTooltipId = useId("indexLanguageTooltipId-");
    const indexTypeTooltipId = useId("indexTypeTooltipId-");

    return (
        <>
            <div className="text-truncate" title={name}>
                {name}
            </div>
            <FlexGrow />
            {disabledReason && (
                <div id={indexDisabledReasonTooltipId} className="pe-1">
                    <UncontrolledTooltip target={indexDisabledReasonTooltipId}>{disabledReason}</UncontrolledTooltip>
                    <Icon icon="warning" color="warning" margin="m-0" id={indexDisabledReasonTooltipId} />
                </div>
            )}
            <div className="index-legend">
                <div id={indexLanguageTooltipId}>
                    <Icon icon={IndexUtils.isCsharpIndex(type) ? "csharp-logo" : "javascript"} margin="m-0" />
                </div>
                <UncontrolledTooltip target={indexLanguageTooltipId} placement="top">
                    <span>{IndexUtils.isCsharpIndex(type) ? <span>C#</span> : <span>JavaScript</span>} index</span>
                </UncontrolledTooltip>
                <div id={indexTypeTooltipId}>
                    <Icon icon={IndexUtils.indexTypeIcon(type)} margin="m-0" />
                </div>
                <UncontrolledTooltip target={indexTypeTooltipId} placement="top">
                    {IndexUtils.formatType(type)}
                </UncontrolledTooltip>
            </div>
        </>
    );
}
