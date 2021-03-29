import operator
import os
import posixpath
import shutil
import time
from subprocess import PIPE, Popen
from tempfile import mkstemp

import paramiko
from tqdm import tqdm

from powersimdata.data_access.profile_helper import ProfileHelper
from powersimdata.utility import server_setup
from powersimdata.utility.helpers import CommandBuilder


class DataAccess:
    """Interface to a local or remote data store."""

    def copy_from(self, file_name, from_dir):
        """Copy a file from data store to userspace.

        :param str file_name: file name to copy.
        :param str from_dir: data store directory to copy file from.
        """
        raise NotImplementedError

    def move_to(self, file_name, to_dir, change_name_to=None):
        """Copy a file from userspace to data store.

        :param str file_name: file name to copy.
        :param str to_dir: data store directory to copy file to.
        :param str change_name_to: new name for file when copied to data store.
        """
        raise NotImplementedError

    def copy(self, src, dest, recursive=False, update=False):
        """Wrapper around cp command which creates dest path if needed

        :param str src: path to original
        :param str dest: destination path
        :param bool recursive: create directories recursively
        :param bool update: only copy if needed
        """
        self.makedir(posixpath.dirname(dest))
        command = CommandBuilder.copy(src, dest, recursive, update)
        return self.execute_command(command)

    def remove(self, target, recursive=False, force=False):
        """Wrapper around rm command

        :param str target: path to remove
        :param bool recursive: delete directories recursively
        :param bool force: remove without confirmation
        """
        command = CommandBuilder.remove(target, recursive, force)
        return self.execute_command(command)

    def _check_file_exists(self, filepath, should_exist=True):
        """Check that file exists (or not) at the given path

        :param str filepath: the full path to the file
        :param bool should_exist: whether the file is expected to exist
        :raises OSError: if the expected condition is not met
        """
        _, _, stderr = self.execute_command(CommandBuilder.list(filepath))
        compare = operator.ne if should_exist else operator.eq
        if compare(len(stderr.readlines()), 0):
            msg = "not found" if should_exist else "already exists"
            raise OSError(f"{filepath} {msg} on server")

    def _check_filename(self, filename):
        """Check that filename is only the name part

        :param str filename: the filename to verify
        :raises ValueError: if filename contains path segments
        """
        if len(os.path.dirname(filename)) != 0:
            raise ValueError(f"Expecting file name but got path {filename}")

    def makedir(self, relative_path):
        """Create paths relative to the instance root

        :param str relative_path: the path, without filename, relative to root
        """
        full_path = posixpath.join(self.root, relative_path)
        return self.execute_command(f"mkdir -p {full_path}")

    def execute_command(self, command):
        """Execute a command locally at the data access.

        :param list command: list of str to be passed to command line.
        """
        raise NotImplementedError

    def execute_command_async(self, command):
        """Execute a command locally at the DataAccess, without waiting for completion.

        :param list command: list of str to be passed to command line.
        """
        raise NotImplementedError

    def checksum(self, relative_path):
        """Return the checksum of the file path, and write the content if the
        server is remote

        :param str relative_path: path relative to root
        :return: (*str*) -- the checksum of the file
        """
        raise NotImplementedError

    def push(self, file_name, checksum, change_name_to=None):
        """Push the file from local to remote root folder, ensuring integrity

        :param str file_name: the file name, located at the local root
        :param str checksum: the checksum prior to download
        :param str change_name_to: new name for file when copied to data store.
        """
        raise NotImplementedError

    def get_profile_version(self, grid_model, kind):
        """Returns available raw profile from blob storage

        :param str grid_model: grid model.
        :param str kind: *'demand'*, *'hydro'*, *'solar'* or *'wind'*.
        :return: (*list*) -- available profile version.
        """
        return ProfileHelper.get_profile_version_cloud(grid_model, kind)

    def close(self):
        """Perform any necessary cleanup for the object."""
        pass


class LocalDataAccess(DataAccess):
    """Interface to shared data volume"""

    def __init__(self, root=None):
        self.root = root if root else server_setup.DATA_ROOT_DIR

    def copy_from(self, file_name, from_dir=None):
        """Copy a file from data store to userspace.

        :param str file_name: file name to copy.
        :param str from_dir: data store directory to copy file from.
        """
        pass

    def push(self, file_name, checksum, change_name_to=None):
        """Nothing to be done due to symlink

        :param str file_name: the file name, located at the local root
        :param str checksum: the checksum prior to download
        :param str change_name_to: new name for file when copied to data store.
        """
        pass

    def checksum(self, relative_path):
        """Return dummy value since this is only required for remote
        environment

        :param str relative_path: path relative to root
        :return: (*str*) -- the checksum of the file
        """
        return "dummy_value"

    def move_to(self, file_name, to_dir, change_name_to=None):
        """Copy a file from userspace to data store.

        :param str file_name: file name to copy.
        :param str to_dir: data store directory to copy file to.
        :param str change_name_to: new name for file when copied to data store.
        """
        self._check_filename(file_name)
        src = posixpath.join(server_setup.LOCAL_DIR, file_name)
        file_name = file_name if change_name_to is None else change_name_to
        dest = posixpath.join(self.root, to_dir, file_name)
        print(f"--> Moving file {src} to {dest}")
        self._check_file_exists(dest, should_exist=False)
        self.copy(src, dest)
        self.remove(src)

    def execute_command(self, command):
        """Execute a command locally at the data access.

        :param list command: list of str to be passed to command line.
        """

        def wrap(s):
            if s is not None:
                return s
            return open(os.devnull)

        proc = Popen(
            command,
            shell=True,
            executable="/bin/bash",
            stdout=PIPE,
            stderr=PIPE,
            text=True,
        )
        return wrap(None), wrap(proc.stdout), wrap(proc.stderr)

    def get_profile_version(self, grid_model, kind):
        """Returns available raw profile from blob storage or local disk

        :param str grid_model: grid model.
        :param str kind: *'demand'*, *'hydro'*, *'solar'* or *'wind'*.
        :return: (*list*) -- available profile version.
        """
        blob_version = super().get_profile_version(grid_model, kind)
        local_version = ProfileHelper.get_profile_version_local(grid_model, kind)
        return list(set(blob_version + local_version))


class SSHDataAccess(DataAccess):
    """Interface to a remote data store, accessed via SSH."""

    _last_attempt = 0

    def __init__(self, root=None):
        """Constructor"""
        self._ssh = None
        self._retry_after = 5
        self.root = server_setup.DATA_ROOT_DIR if root is None else root
        self.local_root = server_setup.LOCAL_DIR

    @property
    def ssh(self):
        """Get or create the ssh connection object, with attempts rate limited.

        :raises IOError: if connection failed or still within retry window
        :return: (*paramiko.SSHClient*) -- the client instance
        """
        should_attempt = time.time() - SSHDataAccess._last_attempt > self._retry_after

        if self._ssh is None:
            if should_attempt:
                try:
                    self._setup_server_connection()
                    return self._ssh
                except:  # noqa
                    SSHDataAccess._last_attempt = time.time()
            msg = f"Could not connect to server, will try again after {self._retry_after} seconds"
            raise IOError(msg)

        return self._ssh

    def _setup_server_connection(self):
        """This function setup the connection to the server."""
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            client.load_system_host_keys()
        except IOError:
            print("Could not find ssh host keys.")
            ssh_known_hosts = input("Provide ssh known_hosts key file =")
            while True:
                try:
                    client.load_system_host_keys(str(ssh_known_hosts))
                    break
                except IOError:
                    print("Cannot read file, try again")
                    ssh_known_hosts = input("Provide ssh known_hosts key file =")

        server_user = server_setup.get_server_user()
        client.connect(
            server_setup.SERVER_ADDRESS,
            username=server_user,
            port=server_setup.SERVER_SSH_PORT,
            timeout=10,
        )

        self._ssh = client

    def copy_from(self, file_name, from_dir=None):
        """Copy a file from data store to userspace.

        :param str file_name: file name to copy.
        :param str from_dir: data store directory to copy file from.
        """
        self._check_filename(file_name)
        from_dir = "" if from_dir is None else from_dir
        to_dir = os.path.join(self.local_root, from_dir)
        os.makedirs(to_dir, exist_ok=True)

        from_path = posixpath.join(self.root, from_dir, file_name)
        to_path = os.path.join(to_dir, file_name)
        self._check_file_exists(from_path, should_exist=True)

        with self.ssh.open_sftp() as sftp:
            print(f"Transferring {file_name} from server")
            cbk, bar = progress_bar(ascii=True, unit="b", unit_scale=True)
            _, tmp_path = mkstemp()
            sftp.get(from_path, tmp_path, callback=cbk)
            shutil.move(tmp_path, to_path)
            bar.close()

    def move_to(self, file_name, to_dir=None, change_name_to=None):
        """Copy a file from userspace to data store.

        :param str file_name: file name to copy.
        :param str to_dir: data store directory to copy file to.
        :param str change_name_to: new name for file when copied to data store.
        :raises FileNotFoundError: if specified file does not exist
        """
        self._check_filename(file_name)
        from_path = os.path.join(self.local_root, file_name)

        if not os.path.isfile(from_path):
            raise FileNotFoundError(
                f"{file_name} not found in {self.local_root} on local machine"
            )

        file_name = file_name if change_name_to is None else change_name_to
        to_dir = "" if to_dir is None else to_dir
        to_path = posixpath.join(self.root, to_dir, file_name)
        self.makedir(to_dir)
        self._check_file_exists(to_path, should_exist=False)

        with self.ssh.open_sftp() as sftp:
            print(f"Transferring {file_name} to server")
            sftp.put(from_path, to_path)

        os.remove(from_path)

    def execute_command(self, command):
        """Execute a command locally at the data access.

        :param list command: list of str to be passed to command line.
        :return: (*tuple*) -- stdin, stdout, stderr of executed command.
        """
        return self.ssh.exec_command(command)

    def execute_command_async(self, command):
        """Execute a command via ssh, without waiting for completion.

        :param list command: list of str to be passed to command line.
        :return: (*subprocess.Popen*) -- the local ssh process
        """
        username = server_setup.get_server_user()
        cmd_ssh = ["ssh", username + "@" + server_setup.SERVER_ADDRESS]
        full_command = cmd_ssh + command
        process = Popen(full_command)
        return process

    def checksum(self, relative_path):
        """Return the checksum of the file path (using sha1sum)

        :param str relative_path: path relative to root
        :return: (*str*) -- the checksum of the file
        """
        full_path = posixpath.join(self.root, relative_path)
        self._check_file_exists(full_path)

        command = f"sha1sum {full_path}"
        _, stdout, _ = self.execute_command(command)
        lines = stdout.readlines()
        return lines[0].strip()

    def push(self, file_name, checksum, change_name_to=None):
        """Push file to server and verify the checksum matches a prior value

        :param str file_name: the file name, located at the local root
        :param str checksum: the checksum prior to download
        :param str change_name_to: new name for file when copied to data store.
        :raises IOError: if command generated stderr
        """
        new_name = file_name if change_name_to is None else change_name_to
        backup = f"{new_name}.temp"
        self.move_to(file_name, change_name_to=backup)

        values = {
            "original": posixpath.join(self.root, new_name),
            "updated": posixpath.join(self.root, backup),
            "lockfile": posixpath.join(self.root, "scenario.lockfile"),
            "checksum": checksum,
        }

        template = "(flock -x 200; \
                prev='{checksum}'; \
                curr=$(sha1sum {original}); \
                if [[ $prev == $curr ]]; then mv {updated} {original} -b; \
                else echo CONFLICT_ERROR 1>&2; fi) \
                200>{lockfile}"

        command = template.format(**values)
        _, _, stderr = self.execute_command(command)

        errors = stderr.readlines()
        if len(errors) > 0:
            for e in errors:
                print(e)
            raise IOError("Failed to push file - most likely a conflict was detected.")

    def close(self):
        """Close the connection that was opened when the object was created."""
        self.ssh.close()


def progress_bar(*args, **kwargs):
    """Creates progress bar

    :param \\*args: variable length argument list passed to the tqdm constructor.
    :param \\*\\*kwargs: arbitrary keyword arguments passed to the tqdm constructor.
    """
    bar = tqdm(*args, **kwargs)
    last = [0]

    def show(a, b):
        bar.total = int(b)
        bar.update(int(a - last[0]))
        last[0] = a

    return show, bar
