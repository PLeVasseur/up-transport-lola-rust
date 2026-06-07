/********************************************************************************
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

#include "up_lola_bridge.h"

#include "score/mw/com/impl/bindings/lola/event_data_storage.h"
#include "score/mw/com/impl/bindings/lola/sample_allocatee_ptr.h"
#include "score/mw/com/impl/plumbing/sample_allocatee_ptr.h"
#include "score/mw/com/runtime.h"
#include "score/mw/com/types.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <unistd.h>

namespace
{

using score::mw::com::DataTypeMetaInfo;
using score::mw::com::EventInfo;
using score::mw::com::GenericProxy;
using score::mw::com::GenericProxyEvent;
using score::mw::com::GenericSkeleton;
using score::mw::com::GenericSkeletonEvent;
using score::mw::com::GenericSkeletonServiceElementInfo;
using score::mw::com::InstanceSpecifier;
using score::mw::com::SampleAllocateePtr;
using score::mw::com::SamplePtr;
using score::mw::com::impl::SampleAllocateePtrView;
using score::mw::com::impl::lola::EventDataStorage;

constexpr std::string_view kDefaultLoggingConfigJson = R"json({
  "appId": "UPLO",
  "appDesc": "up-transport-lola-rust",
  "logLevel": "kWarn",
  "logLevelThresholdConsole": "kWarn",
  "logMode": "kConsole",
  "dynamicDatarouterIdentifiers": true
}
)json";

std::optional<std::string> to_string(const UpLolaStr& value)
{
    if (value.len == 0U)
    {
        return std::string{};
    }
    if (value.data == nullptr)
    {
        return std::nullopt;
    }
    const auto* begin = reinterpret_cast<const char*>(value.data);
    return std::string{begin, value.len};
}

bool is_power_of_two(const std::size_t value)
{
    return value != 0U && (value & (value - 1U)) == 0U;
}

std::optional<std::size_t> align_up(const std::size_t value, const std::size_t alignment)
{
    if (!is_power_of_two(alignment))
    {
        return std::nullopt;
    }
    const auto remainder = value % alignment;
    if (remainder == 0U)
    {
        return value;
    }
    const auto padding = alignment - remainder;
    if (value > std::numeric_limits<std::size_t>::max() - padding)
    {
        return std::nullopt;
    }
    return value + padding;
}

bool is_supported_sample_alignment(const std::size_t alignment)
{
    return is_power_of_two(alignment) && alignment <= alignof(std::max_align_t);
}

bool pointer_is_aligned(const void* const ptr, const std::size_t alignment)
{
    if (ptr == nullptr || !is_supported_sample_alignment(alignment))
    {
        return false;
    }
    return (reinterpret_cast<std::uintptr_t>(ptr) % alignment) == 0U;
}

bool checked_multiply(const std::size_t lhs, const std::size_t rhs, std::size_t& result)
{
    if (lhs != 0U && rhs > std::numeric_limits<std::size_t>::max() / lhs)
    {
        return false;
    }
    result = lhs * rhs;
    return true;
}

std::uint8_t* generic_sample_data(SampleAllocateePtr<void>& sample,
                                  const std::size_t sample_size,
                                  const std::size_t sample_alignment,
                                  const std::size_t max_samples)
{
    auto* const skeleton_sample_ptr = static_cast<std::uint8_t*>(sample.Get());
    if (skeleton_sample_ptr == nullptr || sample_size == 0U || max_samples == 0U ||
        !is_supported_sample_alignment(sample_alignment))
    {
        return nullptr;
    }

    const SampleAllocateePtrView<void> sample_view{sample};
    const auto* const lola_sample = sample_view.As<score::mw::com::impl::lola::SampleAllocateePtr<void>>();
    if (lola_sample == nullptr)
    {
        return pointer_is_aligned(skeleton_sample_ptr, sample_alignment) ? skeleton_sample_ptr : nullptr;
    }

    const auto aligned_sample_size = align_up(sample_size, sample_alignment);
    if (!aligned_sample_size.has_value())
    {
        return nullptr;
    }
    std::size_t total_storage_size{0U};
    if (!checked_multiply(aligned_sample_size.value(), max_samples, total_storage_size))
    {
        return nullptr;
    }
    const auto slot_index = static_cast<std::size_t>(lola_sample->GetReferencedSlot());
    if (slot_index >= max_samples || slot_index > std::numeric_limits<std::size_t>::max() / aligned_sample_size.value())
    {
        return nullptr;
    }

    // S-CORE communication commit 56c36d4059d276e804c143d14012576ddf1b9e25:
    // - score/mw/com/impl/bindings/lola/generic_skeleton_event.h routes GenericSkeletonEvent::Allocate()
    //   through the generic SampleAllocateePtr<void> abstraction used here.
    // - score/mw/com/impl/bindings/lola/event_data_storage.h stores event slots in EventDataStorage::data().
    // - score/mw/com/impl/bindings/lola/generic_proxy_event.h receives raw slot samples from that data array.
    // Keep the owning loan object untouched for Send(), but expose the raw slot storage to Rust so producer and
    // consumer use the same sample bytes.
    const auto slot_offset = slot_index * aligned_sample_size.value();
    if (slot_offset > total_storage_size || sample_size > total_storage_size - slot_offset)
    {
        return nullptr;
    }
    const auto skeleton_sample_address = reinterpret_cast<std::uintptr_t>(skeleton_sample_ptr);
    if (skeleton_sample_address < slot_offset)
    {
        return nullptr;
    }
    auto* const storage = reinterpret_cast<EventDataStorage<std::max_align_t>*>(skeleton_sample_address - slot_offset);
    auto* const raw_slots = reinterpret_cast<std::uint8_t*>(storage->data());
    if (raw_slots == nullptr)
    {
        return nullptr;
    }
    const auto raw_slots_address = reinterpret_cast<std::uintptr_t>(raw_slots);
    if (slot_offset > std::numeric_limits<std::uintptr_t>::max() - raw_slots_address)
    {
        return nullptr;
    }
    auto* const raw_slot = reinterpret_cast<std::uint8_t*>(raw_slots_address + slot_offset);
    return pointer_is_aligned(raw_slot, sample_alignment) ? raw_slot : nullptr;
}

std::optional<std::string> sibling_logging_config_path(const std::string& config_path);
std::optional<std::string> write_default_logging_config();
void configure_logging(const std::string& config_path);

UpLolaStatusCode initialize_runtime_once(const std::string& config_path)
{
    if (config_path.empty())
    {
        return UP_LOLA_STATUS_OK;
    }

    static std::mutex runtime_mutex;
    static bool initialized{false};
    std::lock_guard<std::mutex> lock{runtime_mutex};
    if (initialized)
    {
        return UP_LOLA_STATUS_OK;
    }

    configure_logging(config_path);
    score::mw::com::runtime::RuntimeConfiguration runtime_configuration{config_path.c_str()};
    score::mw::com::runtime::InitializeRuntime(runtime_configuration);
    initialized = true;
    return UP_LOLA_STATUS_OK;
}

std::optional<std::string> sibling_logging_config_path(const std::string& config_path)
{
    if (config_path.empty())
    {
        return std::nullopt;
    }

    const auto separator = config_path.find_last_of("/\\");
    const auto logging_path = separator == std::string::npos ? std::string{"logging.json"}
                                                              : config_path.substr(0U, separator + 1U) + "logging.json";
    std::ifstream logging_config{logging_path};
    if (!logging_config.good())
    {
        return std::nullopt;
    }
    return logging_path;
}

void configure_logging(const std::string& config_path)
{
    const char* const existing_logging_config = std::getenv("MW_LOG_CONFIG_FILE");
    if (existing_logging_config != nullptr && existing_logging_config[0] != '\0')
    {
        return;
    }

    auto logging_path = sibling_logging_config_path(config_path);
    if (!logging_path.has_value())
    {
        logging_path = write_default_logging_config();
    }
    if (logging_path.has_value())
    {
        static_cast<void>(::setenv("MW_LOG_CONFIG_FILE", logging_path->c_str(), 0));
    }
}

std::optional<std::string> write_default_logging_config()
{
    const char* const tmpdir_env = std::getenv("TMPDIR");
    const std::string_view tmpdir = tmpdir_env != nullptr && tmpdir_env[0] != '\0' ? std::string_view{tmpdir_env}
                                                                                  : std::string_view{"/tmp"};
    const auto separator = !tmpdir.empty() && tmpdir.back() == '/' ? std::string{} : std::string{"/"};
    const auto logging_path = std::string{tmpdir} + separator + "up_lola_logging_" + std::to_string(::getpid()) +
                              ".json";

    std::ofstream logging_config{logging_path, std::ios::trunc};
    if (!logging_config.good())
    {
        return std::nullopt;
    }
    logging_config << kDefaultLoggingConfigJson;
    if (!logging_config.good())
    {
        return std::nullopt;
    }
    return logging_path;
}

}  // namespace

struct TxLoanPool;
struct RxSamplePool;

struct UpLolaTxLoan
{
    SampleAllocateePtr<void> sample;
    std::uint8_t* sample_data{nullptr};
    std::size_t sample_size{0U};
    TxLoanPool* pool{nullptr};
    std::size_t pool_index{0U};
    bool in_use{false};
};

struct UpLolaRxSample
{
    SamplePtr<void> sample;
    const std::uint8_t* sample_data{nullptr};
    std::size_t sample_size{0U};
    RxSamplePool* pool{nullptr};
    std::size_t pool_index{0U};
    bool in_use{false};
};

struct TxLoanPool
{
    explicit TxLoanPool(const std::size_t capacity) : slots(capacity)
    {
        for (std::size_t i = 0U; i < slots.size(); ++i)
        {
            slots[i].pool = this;
            slots[i].pool_index = i;
        }
    }

    void add_ref() noexcept
    {
        refs.fetch_add(1U, std::memory_order_relaxed);
    }

    void release_ref() noexcept
    {
        if (refs.fetch_sub(1U, std::memory_order_acq_rel) == 1U)
        {
            delete this;
        }
    }

    UpLolaTxLoan* acquire()
    {
        std::lock_guard<std::mutex> lock{mutex};
        for (auto& slot : slots)
        {
            if (!slot.in_use)
            {
                slot.in_use = true;
                slot.pool = this;
                slot.sample_data = nullptr;
                slot.sample_size = 0U;
                add_ref();
                return &slot;
            }
        }
        return nullptr;
    }

    void release(UpLolaTxLoan* loan) noexcept
    {
        if (loan == nullptr || loan->pool != this)
        {
            return;
        }
        bool was_in_use{false};
        {
            std::lock_guard<std::mutex> lock{mutex};
            if (!loan->in_use)
            {
                return;
            }
            was_in_use = true;
            loan->sample = SampleAllocateePtr<void>{};
            loan->sample_data = nullptr;
            loan->sample_size = 0U;
            loan->pool = nullptr;
            loan->in_use = false;
        }
        if (was_in_use)
        {
            release_ref();
        }
    }

    std::mutex mutex;
    std::atomic<std::size_t> refs{1U};
    std::vector<UpLolaTxLoan> slots;
};

struct RxSamplePool
{
    explicit RxSamplePool(const std::size_t capacity) : slots(capacity)
    {
        for (std::size_t i = 0U; i < slots.size(); ++i)
        {
            slots[i].pool = this;
            slots[i].pool_index = i;
        }
    }

    void add_ref() noexcept
    {
        refs.fetch_add(1U, std::memory_order_relaxed);
    }

    void release_ref() noexcept
    {
        if (refs.fetch_sub(1U, std::memory_order_acq_rel) == 1U)
        {
            delete this;
        }
    }

    UpLolaRxSample* acquire()
    {
        std::lock_guard<std::mutex> lock{mutex};
        for (auto& slot : slots)
        {
            if (!slot.in_use)
            {
                slot.in_use = true;
                slot.pool = this;
                slot.sample_data = nullptr;
                slot.sample_size = 0U;
                add_ref();
                return &slot;
            }
        }
        return nullptr;
    }

    void release(UpLolaRxSample* sample) noexcept
    {
        if (sample == nullptr || sample->pool != this)
        {
            return;
        }
        bool was_in_use{false};
        {
            std::lock_guard<std::mutex> lock{mutex};
            if (!sample->in_use)
            {
                return;
            }
            was_in_use = true;
            sample->sample = SamplePtr<void>{};
            sample->sample_data = nullptr;
            sample->sample_size = 0U;
            sample->pool = nullptr;
            sample->in_use = false;
        }
        if (was_in_use)
        {
            release_ref();
        }
    }

    std::mutex mutex;
    std::atomic<std::size_t> refs{1U};
    std::vector<UpLolaRxSample> slots;
};

struct UpLolaTransport
{
    std::string instance_specifier;
    std::string service_type;
    std::string event_name;
    std::size_t sample_size;
    std::size_t sample_alignment;
    std::size_t max_samples;
    std::mutex mutex;
    std::optional<GenericSkeleton> skeleton;
    GenericSkeletonEvent* skeleton_event{nullptr};
    std::optional<GenericProxy> proxy;
    GenericProxyEvent* proxy_event{nullptr};
    TxLoanPool* tx_pool{nullptr};
    RxSamplePool* rx_pool{nullptr};
    bool subscribed{false};

    ~UpLolaTransport()
    {
        if (tx_pool != nullptr)
        {
            tx_pool->release_ref();
        }
        if (rx_pool != nullptr)
        {
            rx_pool->release_ref();
        }
    }

    UpLolaStatusCode ensure_proxy_locked()
    {
        if (subscribed && proxy_event != nullptr)
        {
            return UP_LOLA_STATUS_OK;
        }

        auto specifier_result = InstanceSpecifier::Create(std::string{instance_specifier});
        if (!specifier_result.has_value())
        {
            return UP_LOLA_STATUS_INVALID_ARGUMENT;
        }

        auto handles_result = GenericProxy::FindService(std::move(specifier_result).value());
        if (!handles_result.has_value())
        {
            return UP_LOLA_STATUS_INTERNAL;
        }
        auto& handles = handles_result.value();
        if (handles.empty())
        {
            return UP_LOLA_STATUS_NOT_FOUND;
        }

        auto proxy_result = GenericProxy::Create(handles.front());
        if (!proxy_result.has_value())
        {
            return UP_LOLA_STATUS_INTERNAL;
        }
        proxy.emplace(std::move(proxy_result).value());

        auto& events = proxy->GetEvents();
        auto event_it = events.find(std::string_view{event_name});
        if (event_it == events.cend())
        {
            proxy.reset();
            return UP_LOLA_STATUS_NOT_FOUND;
        }
        proxy_event = &event_it->second;

        auto subscribe_result = proxy_event->Subscribe(max_samples);
        if (!subscribe_result.has_value())
        {
            proxy_event = nullptr;
            proxy.reset();
            subscribed = false;
            return UP_LOLA_STATUS_INTERNAL;
        }
        subscribed = true;
        return UP_LOLA_STATUS_OK;
    }
};

struct UpLolaSubscriber
{
    std::string instance_specifier;
    std::string event_name;
    std::size_t sample_size;
    std::size_t sample_alignment;
    std::size_t max_samples;
    std::mutex mutex;
    std::optional<GenericProxy> proxy;
    GenericProxyEvent* proxy_event{nullptr};
    RxSamplePool* rx_pool{nullptr};
    bool subscribed{false};

    ~UpLolaSubscriber()
    {
        if (rx_pool != nullptr)
        {
            rx_pool->release_ref();
        }
    }

    UpLolaStatusCode ensure_proxy_locked()
    {
        if (subscribed && proxy_event != nullptr)
        {
            return UP_LOLA_STATUS_OK;
        }

        auto specifier_result = InstanceSpecifier::Create(std::string{instance_specifier});
        if (!specifier_result.has_value())
        {
            return UP_LOLA_STATUS_INVALID_ARGUMENT;
        }

        auto handles_result = GenericProxy::FindService(std::move(specifier_result).value());
        if (!handles_result.has_value())
        {
            return UP_LOLA_STATUS_INTERNAL;
        }
        auto& handles = handles_result.value();
        if (handles.empty())
        {
            return UP_LOLA_STATUS_NOT_FOUND;
        }

        auto proxy_result = GenericProxy::Create(handles.front());
        if (!proxy_result.has_value())
        {
            return UP_LOLA_STATUS_INTERNAL;
        }
        proxy.emplace(std::move(proxy_result).value());

        auto& events = proxy->GetEvents();
        auto event_it = events.find(std::string_view{event_name});
        if (event_it == events.cend())
        {
            proxy.reset();
            return UP_LOLA_STATUS_NOT_FOUND;
        }
        proxy_event = &event_it->second;

        auto subscribe_result = proxy_event->Subscribe(max_samples);
        if (!subscribe_result.has_value())
        {
            proxy_event = nullptr;
            proxy.reset();
            subscribed = false;
            return UP_LOLA_STATUS_INTERNAL;
        }
        subscribed = true;
        return UP_LOLA_STATUS_OK;
    }
};

UpLolaStatusCode up_lola_transport_create(const UpLolaConfig* config, UpLolaTransport** out_transport)
{
    if (config == nullptr || out_transport == nullptr || config->sample_size == 0U ||
        !is_supported_sample_alignment(config->sample_alignment) || config->max_samples == 0U)
    {
        return UP_LOLA_STATUS_INVALID_ARGUMENT;
    }
    *out_transport = nullptr;

    auto instance_specifier = to_string(config->instance_specifier);
    auto service_type = to_string(config->service_type);
    auto event_name = to_string(config->event_name);
    auto mw_com_config_path = to_string(config->mw_com_config_path);
    if (!instance_specifier.has_value() || !service_type.has_value() || !event_name.has_value() ||
        !mw_com_config_path.has_value() || instance_specifier->empty() || service_type->empty() || event_name->empty())
    {
        return UP_LOLA_STATUS_INVALID_ARGUMENT;
    }

    const auto runtime_status = initialize_runtime_once(*mw_com_config_path);
    if (runtime_status != UP_LOLA_STATUS_OK)
    {
        return runtime_status;
    }

    auto specifier_result = InstanceSpecifier::Create(std::string{*instance_specifier});
    if (!specifier_result.has_value())
    {
        return UP_LOLA_STATUS_INVALID_ARGUMENT;
    }

    auto transport = std::make_unique<UpLolaTransport>();
    transport->instance_specifier = std::move(*instance_specifier);
    transport->service_type = std::move(*service_type);
    transport->event_name = std::move(*event_name);
    transport->sample_size = config->sample_size;
    transport->sample_alignment = config->sample_alignment;
    transport->max_samples = config->max_samples;
    transport->tx_pool = new TxLoanPool{config->max_samples};
    transport->rx_pool = new RxSamplePool{config->max_samples};

    const DataTypeMetaInfo data_type_meta_info{config->sample_size, config->sample_alignment};
    const std::vector<EventInfo> events{{transport->event_name, data_type_meta_info}};
    GenericSkeletonServiceElementInfo create_params;
    create_params.events = events;

    auto skeleton_result = GenericSkeleton::Create(std::move(specifier_result).value(), create_params);
    if (!skeleton_result.has_value())
    {
        return UP_LOLA_STATUS_INTERNAL;
    }
    transport->skeleton.emplace(std::move(skeleton_result).value());

    const auto& skeleton_events = transport->skeleton->GetEvents();
    auto event_it = skeleton_events.find(std::string_view{transport->event_name});
    if (event_it == skeleton_events.cend())
    {
        return UP_LOLA_STATUS_NOT_FOUND;
    }
    transport->skeleton_event = const_cast<GenericSkeletonEvent*>(&event_it->second);

    auto offer_result = transport->skeleton->OfferService();
    if (!offer_result.has_value())
    {
        return UP_LOLA_STATUS_INTERNAL;
    }

    *out_transport = transport.release();
    return UP_LOLA_STATUS_OK;
}

void up_lola_transport_destroy(UpLolaTransport* transport)
{
    if (transport == nullptr)
    {
        return;
    }
    if (transport->proxy_event != nullptr && transport->subscribed)
    {
        transport->proxy_event->Unsubscribe();
    }
    if (transport->skeleton.has_value())
    {
        transport->skeleton->StopOfferService();
    }
    delete transport;
}

UpLolaStatusCode up_lola_transport_reserve(UpLolaTransport* transport, UpLolaTxLoan** out_loan)
{
    if (transport == nullptr || out_loan == nullptr || transport->skeleton_event == nullptr)
    {
        return UP_LOLA_STATUS_INVALID_ARGUMENT;
    }
    *out_loan = nullptr;
    std::lock_guard<std::mutex> lock{transport->mutex};

    auto* loan = transport->tx_pool == nullptr ? nullptr : transport->tx_pool->acquire();
    if (loan == nullptr)
    {
        return UP_LOLA_STATUS_RESOURCE_EXHAUSTED;
    }

    auto sample_result = transport->skeleton_event->Allocate();
    if (!sample_result.has_value())
    {
        loan->pool->release(loan);
        return UP_LOLA_STATUS_RESOURCE_EXHAUSTED;
    }

    loan->sample = std::move(sample_result).value();
    loan->sample_size = transport->sample_size;
    loan->sample_data = generic_sample_data(
        loan->sample, transport->sample_size, transport->sample_alignment, transport->max_samples);
    if (loan->sample_data == nullptr)
    {
        loan->pool->release(loan);
        return UP_LOLA_STATUS_INTERNAL;
    }
    *out_loan = loan;
    return UP_LOLA_STATUS_OK;
}

std::uint8_t* up_lola_tx_loan_data(UpLolaTxLoan* loan)
{
    if (loan == nullptr || !loan->in_use || loan->sample_data == nullptr || !loan->sample)
    {
        return nullptr;
    }
    return loan->sample_data;
}

std::size_t up_lola_tx_loan_size(const UpLolaTxLoan* loan)
{
    return loan == nullptr || !loan->in_use ? 0U : loan->sample_size;
}

void up_lola_tx_loan_destroy(UpLolaTxLoan* loan)
{
    if (loan != nullptr && loan->pool != nullptr)
    {
        loan->pool->release(loan);
    }
}

UpLolaStatusCode up_lola_transport_send(UpLolaTransport* transport, UpLolaTxLoan* loan)
{
    if (transport == nullptr || loan == nullptr || transport->skeleton_event == nullptr || !loan->in_use ||
        loan->pool != transport->tx_pool || loan->sample_data == nullptr || !loan->sample)
    {
        return UP_LOLA_STATUS_INVALID_ARGUMENT;
    }
    std::lock_guard<std::mutex> lock{transport->mutex};
    auto send_result = transport->skeleton_event->Send(std::move(loan->sample));
    loan->pool->release(loan);
    if (!send_result.has_value())
    {
        return UP_LOLA_STATUS_INTERNAL;
    }
    return UP_LOLA_STATUS_OK;
}

UpLolaStatusCode up_lola_transport_receive(UpLolaTransport* transport, UpLolaRxSample** out_sample)
{
    if (transport == nullptr || out_sample == nullptr)
    {
        return UP_LOLA_STATUS_INVALID_ARGUMENT;
    }
    *out_sample = nullptr;
    std::lock_guard<std::mutex> lock{transport->mutex};

    const auto proxy_status = transport->ensure_proxy_locked();
    if (proxy_status != UP_LOLA_STATUS_OK)
    {
        return proxy_status;
    }

    SamplePtr<void> received_sample;
    auto get_result = transport->proxy_event->GetNewSamples(
        [&received_sample](SamplePtr<void> sample) noexcept {
            if (!received_sample)
            {
                received_sample = std::move(sample);
            }
        },
        1U);
    if (!get_result.has_value())
    {
        return UP_LOLA_STATUS_INTERNAL;
    }
    if (*get_result == 0U || !received_sample)
    {
        return UP_LOLA_STATUS_NOT_FOUND;
    }

    auto* sample = transport->rx_pool == nullptr ? nullptr : transport->rx_pool->acquire();
    if (sample == nullptr)
    {
        return UP_LOLA_STATUS_RESOURCE_EXHAUSTED;
    }
    sample->sample = std::move(received_sample);
    sample->sample_data = static_cast<const std::uint8_t*>(sample->sample.Get());
    sample->sample_size = transport->sample_size;
    if (sample->sample_data == nullptr || !pointer_is_aligned(sample->sample_data, transport->sample_alignment))
    {
        sample->pool->release(sample);
        return UP_LOLA_STATUS_INTERNAL;
    }
    *out_sample = sample;
    return UP_LOLA_STATUS_OK;
}

UpLolaStatusCode up_lola_subscriber_create(const UpLolaConfig* config, UpLolaSubscriber** out_subscriber)
{
    if (config == nullptr || out_subscriber == nullptr || config->sample_size == 0U ||
        !is_supported_sample_alignment(config->sample_alignment) || config->max_samples == 0U)
    {
        return UP_LOLA_STATUS_INVALID_ARGUMENT;
    }
    *out_subscriber = nullptr;

    auto instance_specifier = to_string(config->instance_specifier);
    auto event_name = to_string(config->event_name);
    auto mw_com_config_path = to_string(config->mw_com_config_path);
    if (!instance_specifier.has_value() || !event_name.has_value() || !mw_com_config_path.has_value() ||
        instance_specifier->empty() || event_name->empty())
    {
        return UP_LOLA_STATUS_INVALID_ARGUMENT;
    }

    const auto runtime_status = initialize_runtime_once(*mw_com_config_path);
    if (runtime_status != UP_LOLA_STATUS_OK)
    {
        return runtime_status;
    }

    auto subscriber = std::make_unique<UpLolaSubscriber>();
    subscriber->instance_specifier = std::move(*instance_specifier);
    subscriber->event_name = std::move(*event_name);
    subscriber->sample_size = config->sample_size;
    subscriber->sample_alignment = config->sample_alignment;
    subscriber->max_samples = config->max_samples;
    subscriber->rx_pool = new RxSamplePool{config->max_samples};

    {
        std::lock_guard<std::mutex> lock{subscriber->mutex};
        const auto proxy_status = subscriber->ensure_proxy_locked();
        if (proxy_status != UP_LOLA_STATUS_OK)
        {
            return proxy_status;
        }
    }

    *out_subscriber = subscriber.release();
    return UP_LOLA_STATUS_OK;
}

void up_lola_subscriber_destroy(UpLolaSubscriber* subscriber)
{
    if (subscriber == nullptr)
    {
        return;
    }
    if (subscriber->proxy_event != nullptr && subscriber->subscribed)
    {
        subscriber->proxy_event->Unsubscribe();
    }
    delete subscriber;
}

UpLolaStatusCode up_lola_subscriber_receive(UpLolaSubscriber* subscriber, UpLolaRxSample** out_sample)
{
    if (subscriber == nullptr || out_sample == nullptr)
    {
        return UP_LOLA_STATUS_INVALID_ARGUMENT;
    }
    *out_sample = nullptr;
    std::lock_guard<std::mutex> lock{subscriber->mutex};

    const auto proxy_status = subscriber->ensure_proxy_locked();
    if (proxy_status != UP_LOLA_STATUS_OK)
    {
        return proxy_status;
    }

    SamplePtr<void> received_sample;
    auto get_result = subscriber->proxy_event->GetNewSamples(
        [&received_sample](SamplePtr<void> sample) noexcept {
            if (!received_sample)
            {
                received_sample = std::move(sample);
            }
        },
        1U);
    if (!get_result.has_value())
    {
        return UP_LOLA_STATUS_INTERNAL;
    }
    if (*get_result == 0U || !received_sample)
    {
        return UP_LOLA_STATUS_NOT_FOUND;
    }

    auto* sample = subscriber->rx_pool == nullptr ? nullptr : subscriber->rx_pool->acquire();
    if (sample == nullptr)
    {
        return UP_LOLA_STATUS_RESOURCE_EXHAUSTED;
    }
    sample->sample = std::move(received_sample);
    sample->sample_data = static_cast<const std::uint8_t*>(sample->sample.Get());
    sample->sample_size = subscriber->sample_size;
    if (sample->sample_data == nullptr || !pointer_is_aligned(sample->sample_data, subscriber->sample_alignment))
    {
        sample->pool->release(sample);
        return UP_LOLA_STATUS_INTERNAL;
    }
    *out_sample = sample;
    return UP_LOLA_STATUS_OK;
}

const std::uint8_t* up_lola_rx_sample_data(const UpLolaRxSample* sample)
{
    if (sample == nullptr || !sample->in_use || sample->sample_data == nullptr || !sample->sample)
    {
        return nullptr;
    }
    return sample->sample_data;
}

std::size_t up_lola_rx_sample_size(const UpLolaRxSample* sample)
{
    return sample == nullptr || !sample->in_use ? 0U : sample->sample_size;
}

void up_lola_rx_sample_destroy(UpLolaRxSample* sample)
{
    if (sample != nullptr && sample->pool != nullptr)
    {
        sample->pool->release(sample);
    }
}
