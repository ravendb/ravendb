import React, { useState } from "react";
import {
    Alert,
    Button,
    InputGroup,
    InputGroupText,
    ListGroup,
    ListGroupItem,
    ListGroupItemHeading,
    Modal,
    ModalBody,
    ModalFooter,
} from "reactstrap";
import { Icon } from "components/common/Icon";
import { RadioToggleWithIcon, RadioToggleWithIconInputItem } from "components/common/RadioToggle";
import Select from "components/common/select/Select";
import { todo } from "common/developmentHelper";

interface ExportIndexesProps {
    toggle: () => void;
}

todo("Feature", "Damian", "Add logic for Export indexes");

export function ExportIndexes(props: ExportIndexesProps) {
    const { toggle } = props;

    const [radioToggleSelectedValue, setRadioToggleSelectedValue] = useState(leftRadioToggleItem.value);

    const selectedIndexes = [
        {
            heading: "Orders",
            items: ["Orders/ByCompany", "Orders/ByShipment/Location", "Orders/Totals"],
        },
        {
            heading: "Products",
            items: ["Products/ByCompany", "Products/ByShipment/Location", "Products/Totals"],
        },
        {
            heading: "Customers",
            items: ["Customers/ByRegion", "Customers/ByPurchaseHistory", "Customers/TotalSpending"],
        },
    ];

    return (
        <Modal
            isOpen
            toggle={toggle}
            size="lg"
            wrapClassName="bs5"
            contentClassName={`modal-border bulge-primary`}
            centered
        >
            <ModalBody className="vstack gap-4 position-relative">
                <Icon icon="index-import" color="primary" className="text-center fs-1" margin="m-0" />
                <div className="lead text-center">
                    You&apos;re about to <span className="fw-bold">export</span> selected indexes
                </div>
                <div className="mx-auto">
                    <RadioToggleWithIcon
                        name={null}
                        leftItem={leftRadioToggleItem}
                        rightItem={rightRadioToggleItem}
                        selectedValue={radioToggleSelectedValue}
                        setSelectedValue={(x) => setRadioToggleSelectedValue(x)}
                    />
                </div>
                {radioToggleSelectedValue === leftRadioToggleItem.value ? (
                    <>
                        <InputGroup>
                            <InputGroupText>
                                <Icon icon="database" margin="m-0" />
                            </InputGroupText>
                            <Select
                                name={null}
                                placeholder="Select destination database"
                                options={[
                                    { label: "t3st", value: 1 },
                                    { label: "d4t4b4se", value: 2 },
                                ]}
                            />
                        </InputGroup>
                    </>
                ) : null}
                <div>
                    <div className="d-flex flex-vertical gap-3">
                        {selectedIndexes.map((collection, index) => (
                            <div key={index}>
                                <ListGroupItemHeading className="mb-1 small-label">
                                    {collection.heading}
                                </ListGroupItemHeading>
                                <ListGroup>
                                    {collection.items.map((item, itemIndex) => (
                                        <ListGroupItem key={itemIndex}>{item}</ListGroupItem>
                                    ))}
                                </ListGroup>
                            </div>
                        ))}
                    </div>
                </div>
                {radioToggleSelectedValue === leftRadioToggleItem.value && (
                    <Alert color="info" className="text-left">
                        <Icon icon="info" />
                        All the conflicting indexes in destination database will be overwritten after the export is done
                    </Alert>
                )}
                <div className="position-absolute m-2 end-0 top-0">
                    <Button close onClick={toggle} />
                </div>
            </ModalBody>
            <ModalFooter>
                <Button type="button" color="link" title="Cancel" className="link-muted" onClick={toggle}>
                    Cancel
                </Button>
                <Button type="submit" color="success" title="Export indexes" className="rounded-pill">
                    <Icon icon="export" />
                    Export indexes{" "}
                    {radioToggleSelectedValue === leftRadioToggleItem.value ? "to database" : "to a file"}
                </Button>
            </ModalFooter>
        </Modal>
    );
}

const leftRadioToggleItem: RadioToggleWithIconInputItem<boolean> = {
    label: "To database",
    value: false,
    iconName: "database",
};

const rightRadioToggleItem: RadioToggleWithIconInputItem<boolean> = {
    label: "To a file",
    value: true,
    iconName: "document",
};
