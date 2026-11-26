import React from "react";
import { useFormContext, useWatch } from "react-hook-form";
import Collapse from "react-bootstrap/Collapse";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { FormAceEditor, FormSwitch } from "components/common/Form";
import Code from "components/common/Code";
import { ImportOptionsFormData } from "components/pages/database/tasks/importData/shared/importOptionsValidation";

export default function TransformScriptCard() {
    const { control } = useFormContext<ImportOptionsFormData>();
    const isUseTransformScript = useWatch({ control, name: "processing.isUseTransformScript" });

    return (
        <>
            <h4 className="mb-2">Data transformation and integrity</h4>
            <div className="card p-4 mb-4">
                <FormSwitch control={control} name="processing.isUseTransformScript">
                    Use transform script{" "}
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                <div className="mb-1 text-center">Transform scripts are written in JavaScript</div>
                                <Code code={codeSample} language="javascript" />
                            </>
                        }
                    >
                        <span onClick={(e) => e.preventDefault()}>
                            <Icon color="info" icon="info" margin="ms-1" />
                        </span>
                    </PopoverWithHoverWrapper>
                </FormSwitch>
                <Collapse in={isUseTransformScript}>
                    <div className="mt-3">
                        <FormAceEditor control={control} name="processing.transformScript" mode="javascript" />
                    </div>
                </Collapse>
            </div>
        </>
    );
}

const codeSample = `const name = this.FirstName;

if (name === "Bob")
    throw 'skip'; // filter-out

this.Freight = 15.3;
    `;
