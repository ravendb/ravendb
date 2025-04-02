import ReactBootstrapModal, { ModalProps as ReactBootstrapModalProps } from "react-bootstrap/Modal";
import { ModalHeaderProps as ReactBootstrapModalHeaderProps } from "react-bootstrap/ModalHeader";
import classNames from "classnames";
import { LoadingView } from "components/common/LoadingView";
import CloseButton from "react-bootstrap/CloseButton";
import React from "react";
import "./Modal.scss";

interface ModalProps extends ReactBootstrapModalProps {
    isLoading?: boolean;
}

export function Modal({ children, container, isLoading, className, ...props }: ModalProps) {
    return (
        <ReactBootstrapModal
            centered
            contentClassName={classNames("position-relative", className)}
            container={container || document.getElementById("bs5-modal")}
            {...props}
        >
            {isLoading && <LoadingView />}
            {!isLoading && children}
        </ReactBootstrapModal>
    );
}

export interface ModalHeaderProps extends ReactBootstrapModalHeaderProps {
    onCloseClick?: () => void;
}

export function ModalHeader({ children, closeButton = true, onCloseClick, className, ...props }: ModalHeaderProps) {
    return (
        <ReactBootstrapModal.Header className={classNames("position-relative", className)} {...props}>
            {closeButton && <CloseButton onClick={onCloseClick} className="position-absolute end-0 m-1 top-0" />}
            {children}
        </ReactBootstrapModal.Header>
    );
}

Modal.Header = ModalHeader;

Modal.Body = ReactBootstrapModal.Body;

Modal.Footer = ReactBootstrapModal.Footer;

export default Modal;
