gnome-terminal -e 'bash -c "sudo killall openvpn; ssh-add ~/.ssh/id_rsa_benchmark; sudo dcos auth login; sudo dcos package install tunnel-cli --cli --yes; sudo SSH_AUTH_SOCK=$SSH_AUTH_SOCK dcos tunnel --verbose vpn; bash"'
