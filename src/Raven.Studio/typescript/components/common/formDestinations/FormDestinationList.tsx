import React from "react";
import { Label } from "reactstrap";
import Local from "./Local";
import AmazonS3 from "./AmazonS3";
import Azure from "./Azure";
import GoogleCloud from "./GoogleCloud";
import AmazonGlacier from "./AmazonGlacier";
import Ftp from "./Ftp";
import { FormDestinations } from "./utils/formDestinationsTypes";
import { useFormContext } from "react-hook-form";

export default function FormDestinationList() {
    const { formState } = useFormContext<FormDestinations>();

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
            {formState.errors?.destinations?.message && (
                <div className="text-danger small">{formState.errors.destinations.message}</div>
            )}
        </>
    );
}
