/// <reference types="react-bootstrap" />
import { BsPrefixRefForwardingComponent } from "react-bootstrap/helpers";
import { ButtonProps, SpinnerProps as ReactBootstrapSpinnerProps } from "react-bootstrap";

type RavenSizes = "xs"

type RavenVariants = "link-muted" | "outline-node" | "outline-shard" | "node" | "shard" | "cloud"


declare module "react-bootstrap/Spinner" {
    export type RavenSpinnerSizes = ReactBootstrapSpinnerProps["size"] | RavenSizes
    
    export interface RavenSpinnerProps extends Omit<ReactBootstrapSpinnerProps, "size"> {
        size?: RavenSpinnerSizes;
    }
    
    declare const Spinner: BsPrefixRefForwardingComponent<"div", RavenSpinnerProps>;
    export = Spinner;
}

declare module "react-bootstrap/Button" {
    export type RavenButtonVariants = ButtonProps["variant"] | RavenVariants
    
    export type RavenButtonSizes = ButtonProps["size"] | RavenSizes
    
    export interface BtnProps extends Omit<ButtonProps, "size"> {
        size?: RavenButtonSizes;
        variant?: RavenButtonVariants
    }
    
    declare const Button: BsPrefixRefForwardingComponent<"button", BtnProps>;
    export = Button;
}
