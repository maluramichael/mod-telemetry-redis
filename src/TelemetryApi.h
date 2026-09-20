#pragma once
#include <string>
#include <vector>
#include <utility>
namespace DadTelemetry {
    void Emit(std::string const& type, std::vector<std::pair<std::string,std::string>> const& fields);
    bool Enabled();
}
