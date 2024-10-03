import React from "react";
import { Button } from "reactstrap";
import { Icon } from "components/common/Icon";
import { FlexGrow } from "components/common/FlexGrow";

interface ClusterDebugPaginationProps {}

export default function ClusterDebugPagination(props: ClusterDebugPaginationProps) {
    return (
        <div className="hstack gap-1 justify-content-between mt-4 flex-wrap">
            <Button>
                <Icon icon="arrow-thin-left" /> Previous
            </Button>
            <FlexGrow />
            <div className="hstack gap-1">
                <Button color="primary">
                    <Icon icon="arrow-thin-top" /> Jump to the committed index
                </Button>
                <Button>
                    Next <Icon icon="arrow-thin-right" margin="ms-1" />
                </Button>
            </div>
        </div>
    );
}
