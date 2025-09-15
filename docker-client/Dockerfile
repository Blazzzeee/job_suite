FROM alpine:latest

# Install OpenSSH
RUN apk update && apk add --no-cache openssh

# Create SSH user and set password (passed as build arguments)
ARG SSH_USER=clientuser
ARG SSH_PASSWORD=changeme

RUN adduser -D $SSH_USER && \
    echo "$SSH_USER:$SSH_PASSWORD" | chpasswd

# Set up SSH server
RUN mkdir /var/run/sshd && \
    ssh-keygen -A

# Allow password-based login
RUN sed -i 's/^#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config && \
    sed -i 's/^#PermitRootLogin.*/PermitRootLogin no/' /etc/ssh/sshd_config && \
    echo "MaxSessions 100000" >> /etc/ssh/sshd_config

# Expose SSH port
EXPOSE 22

# Start SSH daemon
CMD ["/usr/sbin/sshd", "-D"]

