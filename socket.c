#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <arpa/inet.h>

#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>

int main(int argc, char **argv)
{
    int server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket == -1) 
    {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    struct sockaddr_in server_address;
    memset(&server_address, 0, sizeof(server_address));
    server_address.sin_family = AF_INET;
    server_address.sin_addr.s_addr = INADDR_ANY; // Use any available network interface
    server_address.sin_port = htons(8080);      // Port number

    if (bind(server_socket, (struct sockaddr*)&server_address, sizeof(server_address)) == -1) 
    {
        perror("Bind failed");
        exit(EXIT_FAILURE);
    }

    if (listen(server_socket, 5) == -1) 
    {
        perror("Listen failed");
        exit(EXIT_FAILURE);
    }

    char buffer[1024];
    memset(buffer, 0, sizeof(buffer));
    ssize_t bytes_received = recv(client_socket, buffer, sizeof(buffer), 0);
    if (bytes_received == -1) 
    {
        perror("Receive failed");
        exit(EXIT_FAILURE);
    }

    printf("Received data: %s\n", buffer);

    const char* message = "Hello, client!";
    ssize_t bytes_sent = send(client_socket, message, strlen(message), 0);
    if (bytes_sent == -1) 
    {
        perror("Send failed");
        exit(EXIT_FAILURE);
    }

    close(client_socket);
    close(server_socket);

    return EXIT_SUCCESS;
}