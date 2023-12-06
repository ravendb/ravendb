import React, { useEffect } from "react";
import { Label } from "reactstrap";
import Local from "./Local";
import AmazonS3 from "./AmazonS3";
import Azure from "./Azure";
import GoogleCloud from "./GoogleCloud";
import AmazonGlacier from "./AmazonGlacier";
import Ftp from "./Ftp";
import { FormDestinations } from "./formDestinationsUtils";
import { useFormContext, useWatch } from "react-hook-form";
import { FormDataWithCustomError } from "components/models/common";

type FormData = FormDataWithCustomError<FormDestinations>;

export default function FormDestinationList() {
    const { control, formState, setError, clearErrors } = useFormContext<FormData>();
    const { local, s3, azure, googleCloud, glacier, ftp } = useWatch({ control });

    useEffect(() => {
        const allIsEnabled = [
            azure.isEnabled,
            ftp.isEnabled,
            glacier.isEnabled,
            googleCloud.isEnabled,
            local.isEnabled,
            s3.isEnabled,
        ];

        console.log("kalczur allIsEnabled", allIsEnabled);
        if (allIsEnabled.every((isEnabled) => !isEnabled)) {
            console.log("kalczur set error call");
            setError("customError", {
                message: "Please select at least one destination",
            });
        } else {
            clearErrors("customError");
        }
    }, [
        azure.isEnabled,
        ftp.isEnabled,
        glacier.isEnabled,
        googleCloud.isEnabled,
        local.isEnabled,
        s3.isEnabled,
        clearErrors,
        setError,
    ]);

    return (
        <>
            <Label className="mt-3 mb-0 md-label">Destinations</Label>
            <div className="vstack gap-1">
                <Local />
                <AmazonS3 />
                <Azure />
                <GoogleCloud />
                <AmazonGlacier />
                <Ftp />
            </div>
            {formState.errors?.customError && (
                <div className="text-danger small">{formState.errors.customError.message}</div>
            )}
        </>
    );
}
