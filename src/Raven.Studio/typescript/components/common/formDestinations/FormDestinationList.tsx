import React from "react";
import Local from "./Local";
import AmazonS3 from "./AmazonS3";
import Azure from "./Azure";
import GoogleCloud from "./GoogleCloud";
import AmazonGlacier from "./AmazonGlacier";
import Ftp from "./Ftp";
import { FormDestinations } from "./utils/formDestinationsTypes";
import { useFormContext } from "react-hook-form";
import { FormLabel } from "components/common/Form";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface FormDestinationListProps {
    isForNewConnection: boolean;
}

export default function FormDestinationList({ isForNewConnection }: FormDestinationListProps) {
    const { formState } = useFormContext<FormDestinations>();
    const t = useStudioTranslation("formDestinations");

    return (
        <>
            <FormLabel>{t("destinations")}</FormLabel>
            <div className="vstack gap-1">
                <Local />
                <AmazonS3 />
                <Azure />
                <GoogleCloud isForNewConnection={isForNewConnection} />
                <AmazonGlacier />
                <Ftp />
            </div>
            {formState.errors?.destinations?.message && (
                <div className="text-danger small">{formState.errors.destinations.message}</div>
            )}
        </>
    );
}
