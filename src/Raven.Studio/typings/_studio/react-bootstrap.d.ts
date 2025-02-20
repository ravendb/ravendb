/// <reference types="react-bootstrap" />
import { BsPrefixRefForwardingComponent } from "react-bootstrap/helpers";
import { ButtonProps } from "react-bootstrap";

declare module "react-bootstrap/Button" {
    export type RavenButtonVariants = ButtonProps["variant"] | "link-muted" | "outline-node" | "outline-shard" | "node" | "shard" | "cloud"
    
    export type RavenButtonSizes = ButtonProps["size"] | "xs"
    
    export interface BtnProps extends Omit<ButtonProps, "size"> {
        size?: RavenButtonSizes;
        variant?: RavenButtonVariants
    }
    
    declare const Button: BsPrefixRefForwardingComponent<"button", BtnProps>;
    export = Button;
}
