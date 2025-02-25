/// <reference types="react-bootstrap" />
import { BsPrefixRefForwardingComponent } from "react-bootstrap/helpers";
import { ButtonProps } from "react-bootstrap/Button";
import { SpinnerProps as ReactBootstrapSpinnerProps } from "react-bootstrap/Spinner";
import { BadgeProps } from "react-bootstrap/Badge";

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

type RavenBadgeFadedVariants = | "faded-primary"
  | "faded-secondary"
  | "faded-success"
  | "faded-warning"
  | "faded-danger"
  | "faded-info"
  | "faded-progress"
  | "faded-node"
  | "faded-shard"
  | "faded-orchestrator"
  | "faded-dark"
  | "faded-light"
  | "faded-muted"
  | "faded-developer"
  | "faded-enterprise"
  | "faded-professional";

type RavenBadgeVariants = | "node"
  | "shard"
  | "cloud"
  | "progress"
  | "orchestrator"
  | "muted"
  | "developer"
  | "enterprise"
  | "professional"

declare module "react-bootstrap/Badge" {
    export type RavenBadgeBgVariants = BadgeProps["bg"] | RavenBadgeVariants | RavenBadgeFadedVariants
    
    export interface RavenBadgeProps extends Omit<BadgeProps, "bg"> {
        bg?: RavenBadgeBgVariants;
    }
    
    declare const Badge: BsPrefixRefForwardingComponent<"span", RavenBadgeProps>;
    export = Badge;
}
