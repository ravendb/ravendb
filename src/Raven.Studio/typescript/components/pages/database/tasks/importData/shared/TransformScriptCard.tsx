import React from "react";
import { useFormContext, useWatch } from "react-hook-form";
import Collapse from "react-bootstrap/Collapse";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { FormAceEditor, FormSwitch } from "components/common/Form";
import Code from "components/common/Code";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import { ImportOptionsFormData } from "components/pages/database/tasks/importData/shared/importOptionsValidation";

export default function TransformScriptCard() {
    const t = useStudioTranslation("importData");
    const { control } = useFormContext<ImportOptionsFormData>();
    const isUseTransformScript = useWatch({ control, name: "processing.isUseTransformScript" });

    return (
        <>
            <h4 className="mb-2">{t("transformScript.title")}</h4>
            <div className="card p-4 mb-4">
                <FormSwitch control={control} name="processing.isUseTransformScript">
                    {t("transformScript.useScript")}{" "}
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                <div className="mb-1 text-center">{t("transformScript.info")}</div>
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
