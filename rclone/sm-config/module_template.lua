help([[
Rclone
A command-line program to sync, copy, mount and serve files between local
storage and over 70 cloud providers (S3, Google Drive, Dropbox, SFTP, ...).
https://rclone.org/
]])

whatis("Name: rclone")
whatis("Version: {{{INSTALL_VERSION}}}")
whatis("URL: https://rclone.org/")

-- the archive root holds the bare `rclone` binary (plus README and manpage),
-- so prepend the install directory itself, not `bin`
prepend_path("PATH", "{{{PATH}}}")
