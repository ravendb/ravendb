import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import React from "react";
import { ModalFooter, Button } from "reactstrap";
import { connectionStringsActions } from "../../store/connectionStringsSlice";
import { useDispatch } from "react-redux";

interface ConnectionStringFormFooterProps {
    isSubmitting: boolean;
}

export default function ConnectionStringFormFooter({ isSubmitting }: ConnectionStringFormFooterProps) {
    const dispatch = useDispatch();

    return (
        <ModalFooter>
            <Button
                type="button"
                color="link"
                className="link-muted"
                onClick={() => dispatch(connectionStringsActions.closeEditConnectionModal())}
                title="Cancel"
            >
                Cancel
            </Button>
            <ButtonWithSpinner
                type="submit"
                color="success"
                title="Save credentials"
                icon="save"
                isSpinning={isSubmitting}
            >
                Save connection string
            </ButtonWithSpinner>
        </ModalFooter>
    );
}
