package com.bsi.peaks.server.es.modification;

import com.bsi.peaks.server.es.UserManager;
import com.bsi.peaks.event.modification.ModificationEvent;
import com.bsi.peaks.event.modification.ModificationEvent.ModificationAdded;
import com.bsi.peaks.event.modification.ModificationEvent.ModificationDeleted;
import com.bsi.peaks.event.modification.ModificationEvent.ModificationUpdated;
import com.bsi.peaks.event.modification.ModificationEvent.PermissionGranted;
import com.bsi.peaks.event.modification.ModificationState;
import com.bsi.peaks.model.proto.Modification;
import com.bsi.peaks.model.proto.ModificationCategory;
import com.bsi.peaks.model.system.User;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public class ModificationStateWrapper {

    private ModificationState state;

    public ModificationStateWrapper() {
        this(ModificationState.getDefaultInstance());
    }

    public ModificationStateWrapper(ModificationState state) {
        this.state = state;
    }

    public Optional<Modification> getModificationByName(String name) {
        return Optional.of(this.state.getModificationsMap().get(name));
    }

    public List<Modification> getModificationByName(List<String> names) {
        return names.stream()
                .map(name -> this.getModificationByName(name).orElseThrow(() -> new IllegalStateException("Modification not found")))
                .collect(Collectors.toList());
    }

    public Map<String, String> getModificationPermissionsByName(List<String> names) {
        return names.stream()
            .map(n -> {
                getPermissionByName(n).orElseThrow(IllegalStateException::new); //verify n exists
                return n;
            })
            .collect(Collectors.toMap(n -> n, n -> state.getPermissionsMap().get(n)));
    }

    public List<Modification> getModificationsByUser(UUID userId, User.Role role) {
        return this.state.getModificationsMap().values().stream()
                .filter(modification -> (role != null && (role.equals(User.Role.SYS_ADMIN)) ||
                        userId.toString().equals(UserManager.ADMIN_USERID) ||
                        this.state.getPermissionsMap().get(modification.getName()).equals(userId.toString())) ||
                        this.state.getPermissionsMap().get(modification.getName()).equals(UserManager.ADMIN_USERID))
                .collect(Collectors.toList());
    }

    public Optional<String> getPermissionByName(String name) {
        return Optional.ofNullable(this.state.getPermissionsMap().get(name));
    }

    public List<Modification> getAllBuiltInModifications() {
        return this.state.getModificationsMap().values().stream()
                .filter(modification -> modification.getBuiltin() == true)
                .collect(Collectors.toList());
    }

    public Map<String, String> getAllBuiltInModificationsPermissions() {
        return this.state.getModificationsMap().values().stream()
            .filter(modification -> modification.getBuiltin() && getPermissionByName(modification.getName()).isPresent())
            .collect(Collectors.toMap(m -> m.getName(), m -> getPermissionByName(m.getName()).get()));
    }

    public void validateAddModification(ModificationAdded modificationAdded) throws IllegalStateException {
        Modification modification = modificationAdded.getModification();
        if (this.state.getModificationsMap().containsKey(modification.getName())) {
            throw new IllegalArgumentException("Duplicate modification name is not allowed");
        }
    }

    public void validateDeleteModification(ModificationEvent event) throws IllegalStateException {
        ModificationDeleted modificationDeleted = event.getModificationDeleted();
        List<String> modifications = this.getModificationsByUser(UUID.fromString(event.getUserId()), null).stream()
                 .map(Modification::getName)
                .collect(Collectors.toList());
        String modificationDeletedName = modificationDeleted.getName();
        if (!modifications.contains(modificationDeletedName)) {
            throw new IllegalArgumentException("Modification to delete does not exist");
        }

        if (getModificationByName(modificationDeletedName)
            .orElseThrow(() -> new IllegalStateException("Modification to delete does not exist"))
            .getBuiltin()
        ) {
            throw new IllegalArgumentException("Modification is built in, cannot be deleted");
        }
    }

    public void validateGrantPermission(ModificationEvent event) throws IllegalStateException {
         PermissionGranted permissionGranted = event.getPermissionGranted();
         final String name = permissionGranted.getName();
         final String oldOwnerId = event.getUserId();
         List<String> modifications = this.getModificationsByUser(UUID.fromString(oldOwnerId), null).stream()
                 .map(Modification::getName)
                 .collect(Collectors.toList());
         if (!this.state.getModificationsMap().containsKey(name) || !this.state.getPermissionsMap().containsKey(name)) {
             throw new IllegalStateException("Modification to grant permission does not exist");
         }
     }

    public ModificationStateWrapper update(ModificationEvent event) throws IllegalStateException {
        switch (event.getEventCase()) {
            case MODIFICATIONADDED:
                return addModification(event);
            case MODIFICATIONDELETED:
                return deleteModification(event);
            case MODIFICATIONUPDATED:
                return updateModification(event);
            case PERMISSIONGRANTED:
                return grantPermission(event);
            case CATEGORYSET:
                return this; //do nothing deprecated event
            default:
                throw new IllegalArgumentException("Invalid modification event");
        }
    }

    public ModificationStateWrapper addModification(ModificationEvent event) {
        ModificationAdded modificationAdded = event.getModificationAdded();
        Modification modification = modificationAdded.getModification();
        this.state = this.state.toBuilder()
                .putModifications(modification.getName(), modification)
                .putPermissions(modification.getName(), event.getUserId())
                .build();
        return new ModificationStateWrapper(this.state);
    }

    public ModificationStateWrapper deleteModification(ModificationEvent event) {
        ModificationDeleted modificationDeleted = event.getModificationDeleted();
        this.state = this.state.toBuilder()
                .removeModifications(modificationDeleted.getName())
                .removePermissions(modificationDeleted.getName())
                .build();
        return new ModificationStateWrapper(this.state);
    }

    public ModificationStateWrapper updateModification(ModificationEvent event) {
        ModificationUpdated modificationUpdated = event.getModificationUpdated();
        this.state = this.state.toBuilder()
                .removeModifications(modificationUpdated.getName())
                .putModifications(modificationUpdated.getModification().getName(), modificationUpdated.getModification())
                .build();
        return new ModificationStateWrapper(this.state);
    }

    public ModificationStateWrapper grantPermission(ModificationEvent event) {
        PermissionGranted permissionGranted = event.getPermissionGranted();
        this.state = this.state.toBuilder()
                .removePermissions(permissionGranted.getName())
                .putPermissions(permissionGranted.getName(), permissionGranted.getNewOwnerId())
                .build();
        return new ModificationStateWrapper(this.state);
    }
}
