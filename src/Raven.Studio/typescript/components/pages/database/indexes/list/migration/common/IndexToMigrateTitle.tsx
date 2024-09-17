import { Icon } from "components/common/Icon";
import useId from "components/hooks/useId";
import { IndexSharedInfo } from "components/models/indexes";
import IndexUtils from "components/utils/IndexUtils";
import React from "react";
import { UncontrolledTooltip } from "reactstrap";

const jsImg = require("Content/img/javascript.svg");
const csharpImg = require("Content/img/csharp-logo.svg");

interface IndexToMigrateTitleProps {
    index: Raven.Client.Documents.Indexes.IndexDefinition | IndexSharedInfo;
    disabledReason?: string;
}

export default function IndexToMigrateTitle({ index, disabledReason }: IndexToMigrateTitleProps) {
    const name = "Name" in index ? index.Name : index.name;
    const type = "Type" in index ? index.Type : index.type;

    const tooltipId = useId("tooltipId-");

    return (
        <div className="d-flex gap-1 align-items-center w-100">
            {IndexUtils.isCsharpIndex(type) ? (
                <img src={csharpImg} alt="C# Index" style={{ width: "22px", filter: "contrast(0)" }} />
            ) : (
                <img src={jsImg} alt="JavaScript Index" style={{ width: "22px", filter: "contrast(0)" }} />
            )}
            <div className="text-truncate" title={name}>
                {name}
            </div>
            {disabledReason && (
                <>
                    <UncontrolledTooltip target={tooltipId}>{disabledReason}</UncontrolledTooltip>
                    <Icon icon="warning" color="warning" margin="m-0" id={tooltipId} />
                </>
            )}
            <div className="ms-auto text-nowrap">
                <Icon icon={IndexUtils.indexTypeIcon(type)} />
                {IndexUtils.formatType(type)}
            </div>
        </div>
    );
}
