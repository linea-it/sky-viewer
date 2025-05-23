import { api } from "./Api";

export const getLoggedUser = () => {
  return api.get("users/me/");
}

export const LogoutUser = () => {
  return api.get("logout/");
}
