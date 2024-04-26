import React from "react";
import { Input } from "reactstrap";
import { FlexGrow } from "components/common/FlexGrow";
import { Icon } from "components/common/Icon";
import { SelectIndexErrorsDropdown } from "components/pages/database/indexes/errors/SelectIndexErrorsDropdown";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";

interface FilterIndexErrorProps {}

export default function FilterIndexErrors(props: FilterIndexErrorProps) {
    const indexesList = [
        { name: "Companies-StockPrices-TradeVolumeByMonth", errorCount: 0 },
        { name: "Companies-ThisOneIsErrored-Something", errorCount: 500, erroredAction: "Map" },
        { name: "Orders-ByCompany", errorCount: 0 },
        { name: "Orders-ByShipment-Location", errorCount: 0 },
        { name: "Orders-Totals", errorCount: 0 },
        { name: "Product-Rating", errorCount: 0 },
        { name: "Product-Search", errorCount: 0 },
        { name: "Products-ByUnitOnStock", errorCount: 0 },
    ];
    return (
        <div className="hstack flex-wrap align-items-end gap-2 mb-3 justify-content-end">
            <div className="flex-grow">
                <div className="small-label ms-1 mb-1">Filter by error column</div>
                <div className="clearable-input">
                    <Input
                        type="text"
                        accessKey="/"
                        placeholder="e.g. mapping function"
                        title="Filter index errors"
                        className="filtering-input rounded-pill"
                        value={null}
                    />
                </div>
            </div>
            <FlexGrow />
            <SelectIndexErrorsDropdown indexesList={indexesList} dropdownType="indexNames" />
            <SelectIndexErrorsDropdown indexesList={indexesList} dropdownType="erroredActions" />
            <ButtonWithSpinner isSpinning={false} color="primary">
                <Icon icon="refresh" /> Refresh
            </ButtonWithSpinner>
        </div>
    );
}
