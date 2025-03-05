import { Icon } from "components/common/Icon";
import React, { useEffect } from "react";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Button from "react-bootstrap/Button";
import { CreateDatabaseFromBackupFormData } from "../createDatabaseFromBackupValidation";
import { useFormContext, useWatch } from "react-hook-form";
import { FormInput } from "components/common/Form";

const fromBackupImg = require("Content/img/createDatabase/from-backup.svg");

export default function CreateDatabaseFromBackupStepBasicInfo() {
    const { control, setValue, setFocus } = useFormContext<CreateDatabaseFromBackupFormData>();
    const {
        basicInfoStep: { isSharded },
    } = useWatch({ control });

    // Focus the database name input when the step is loaded
    useEffect(() => {
        setFocus("basicInfoStep.databaseName");
    }, [setFocus]);

    return (
        <>
            <div className="d-flex justify-content-center">
                <img src={fromBackupImg} alt="" className="step-img" />
            </div>

            <h2 className="text-center mb-4">Restore from backup</h2>

            <Row>
                <Col lg={{ offset: 2, span: 8 }}>
                    <FormInput
                        type="text"
                        control={control}
                        name="basicInfoStep.databaseName"
                        id="DbName"
                        placeholder="Database Name"
                        autoComplete="off"
                    />
                </Col>
            </Row>

            <Row className="mt-2 gy-xs">
                <Col sm="6" lg={{ offset: 2, span: 4 }}>
                    <Button
                        active={!isSharded}
                        onClick={() => setValue("basicInfoStep.isSharded", false)}
                        className=" me-2 px-4 pt-3 w-100"
                        variant="outline-node"
                    >
                        <Icon icon="database" margin="m-0" className="fs-2" />
                        <br />
                        Regular database
                    </Button>
                </Col>
                <Col sm="6" lg="4">
                    <Button
                        active={isSharded}
                        onClick={() => setValue("basicInfoStep.isSharded", true)}
                        variant="outline-shard"
                        className="px-4 pt-3 w-100"
                    >
                        <Icon icon="sharding" margin="m-0" className="fs-2" />
                        <br />
                        Sharded database
                    </Button>
                </Col>
            </Row>
        </>
    );
}
